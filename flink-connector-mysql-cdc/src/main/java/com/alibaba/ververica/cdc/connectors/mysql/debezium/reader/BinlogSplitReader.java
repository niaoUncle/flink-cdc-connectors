/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.debezium.reader;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.MySqlBinlogSplitReadTask;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getBinlogPosition;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getSplitKey;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.getTableId;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isDataChangeRecord;

/**
 * A Debezium binlog reader implementation that also support reads binlog and filter overlapping
 * snapshot data that {@link SnapshotSplitReader} read.
 */
public class BinlogSplitReader implements DebeziumReader<SourceRecord, MySqlSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(BinlogSplitReader.class);
    private final StatefulTaskContext statefulTaskContext;
    private final ExecutorService executor;

    private volatile boolean currentTaskRunning;
    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private MySqlBinlogSplitReadTask binlogSplitReadTask;
    private MySqlSplit currentTableSplit;
    // tableId -> List[splitKeyStart, splitKeyEnd, splitHighWatermark]
    private Map<TableId, List<Tuple3<Object[], Object[], BinlogOffset>>> finishedSplitsInfo;
    // tableId -> the max splitHighWatermark
    private Map<TableId, BinlogOffset> maxSplitHighWatermarkMap;

    public BinlogSplitReader(StatefulTaskContext statefulTaskContext, int subTaskId) {
        this.statefulTaskContext = statefulTaskContext;
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-reader-" + subTaskId).build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.currentTaskRunning = false;
    }

    public void submitSplit(MySqlSplit mySQLSplit) {
        this.currentTableSplit = mySQLSplit;
        configureFilter();
        statefulTaskContext.configure(currentTableSplit);
        this.queue = statefulTaskContext.getQueue();
        final MySqlOffsetContext mySqlOffsetContext = statefulTaskContext.getOffsetContext();
        mySqlOffsetContext.setBinlogStartPoint(
                currentTableSplit.getOffset().getFilename(),
                currentTableSplit.getOffset().getPosition());
        this.binlogSplitReadTask =
                new MySqlBinlogSplitReadTask(
                        statefulTaskContext.getConnectorConfig(),
                        mySqlOffsetContext,
                        statefulTaskContext.getConnection(),
                        statefulTaskContext.getDispatcher(),
                        statefulTaskContext.getErrorHandler(),
                        StatefulTaskContext.getClock(),
                        statefulTaskContext.getTaskContext(),
                        (MySqlStreamingChangeEventSourceMetrics)
                                statefulTaskContext.getStreamingChangeEventSourceMetrics(),
                        statefulTaskContext
                                .getTopicSelector()
                                .topicNameFor(currentTableSplit.getTableId()),
                        currentTableSplit);

        executor.submit(
                () -> {
                    try {
                        currentTaskRunning = true;
                        binlogSplitReadTask.execute(new BinlogSplitChangeEventSourceContextImpl());
                    } catch (Exception e) {
                        currentTaskRunning = false;
                        LOG.error(
                                String.format(
                                        "Execute binlog read task for mysql split %s fail",
                                        currentTableSplit),
                                e);
                        e.printStackTrace();
                    }
                });
    }

    private class BinlogSplitChangeEventSourceContextImpl
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return currentTaskRunning;
        }
    }

    @Override
    public boolean isIdle() {
        return currentTableSplit == null || !currentTaskRunning;
    }

    @Nullable
    @Override
    public Iterator<SourceRecord> pollSplitRecords() throws InterruptedException {
        final List<SourceRecord> sourceRecords = new ArrayList<>();
        if (currentTaskRunning) {
            List<DataChangeEvent> batch = queue.poll();
            for (DataChangeEvent event : batch) {
                if (shouldEmit(event.getRecord())) {
                    sourceRecords.add(event.getRecord());
                }
            }
        }
        return sourceRecords.iterator();
    }

    @Override
    public void close() {
        try {
            if (statefulTaskContext.getConnection() != null) {
                statefulTaskContext.getConnection().close();
            }
            if (statefulTaskContext.getBinaryLogClient() != null) {
                statefulTaskContext.getBinaryLogClient().disconnect();
            }
        } catch (Exception e) {
            LOG.error("Close binlog reader error", e);
        }
    }

    /**
     * Returns the record should emit or not.
     *
     * <p>The watermark signal algorithm is the binlog split reader only sends the binlog event that
     * belongs to its finished snapshot splits. For each snapshot split, the binlog event is valid
     * since the offset is after its high watermark.
     *
     * <pre> E.g: the data input is :
     *    snapshot-split-0 info : [0,    1024) highWatermark0
     *    snapshot-split-1 info : [1024, 2048) highWatermark1
     *  the data output is:
     *  only the binlog event belong to [0,    1024) and offset is after highWatermark0 should send,
     *  only the binlog event belong to [1024, 2048) and offset is after highWatermark1 should send.
     * </pre>
     */
    private boolean shouldEmit(SourceRecord sourceRecord) {
        if (isDataChangeRecord(sourceRecord)) {
            TableId tableId = getTableId(sourceRecord);
            BinlogOffset position = getBinlogPosition(sourceRecord);
            // aligned, all snapshot splits of the table has reached max highWatermark
            if (position.isAtOrBefore(maxSplitHighWatermarkMap.get(tableId))) {
                return true;
            }
            Object[] key =
                    getSplitKey(
                            currentTableSplit.getSplitBoundaryType(),
                            sourceRecord,
                            statefulTaskContext.getSchemaNameAdjuster());
            for (Tuple3<Object[], Object[], BinlogOffset> splitInfo :
                    finishedSplitsInfo.get(tableId)) {
                if (RecordUtils.splitKeyRangeContains(key, splitInfo.f0, splitInfo.f1)
                        && position.isAtOrBefore(splitInfo.f2)) {
                    return true;
                }
            }
            // not in the monitored splits scope, do not emit
            return false;
        }
        // always send the schema change event and signal event
        // we need record them to state of Flink
        return true;
    }

    private void configureFilter() {
        List<Tuple5<TableId, String, Object[], Object[], BinlogOffset>> finishedSplitsInfo =
                currentTableSplit.getFinishedSplitsInfo();
        Map<TableId, List<Tuple3<Object[], Object[], BinlogOffset>>> splitsInfoMap =
                new HashMap<>();
        Map<TableId, BinlogOffset> tableIdBinlogPositionMap = new HashMap<>();

        for (Tuple5<TableId, String, Object[], Object[], BinlogOffset> finishedSplitInfo :
                finishedSplitsInfo) {
            TableId tableId = finishedSplitInfo.f0;
            List<Tuple3<Object[], Object[], BinlogOffset>> list =
                    splitsInfoMap.getOrDefault(tableId, new ArrayList<>());
            list.add(Tuple3.of(finishedSplitInfo.f2, finishedSplitInfo.f3, finishedSplitInfo.f4));
            splitsInfoMap.put(tableId, list);

            BinlogOffset highWatermark = finishedSplitInfo.f4;
            BinlogOffset maxHighWatermark = tableIdBinlogPositionMap.get(tableId);
            if (maxHighWatermark == null || highWatermark.isAtOrBefore(maxHighWatermark)) {
                tableIdBinlogPositionMap.put(tableId, highWatermark);
            }
        }
        this.finishedSplitsInfo = splitsInfoMap;
        this.maxSplitHighWatermarkMap = tableIdBinlogPositionMap;
    }
}
