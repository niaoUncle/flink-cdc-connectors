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

package com.alibaba.ververica.cdc.connectors.mysql.source.split;

import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.debezium.internal.SchemaRecord;
import io.debezium.relational.TableId;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * State of the reader, essentially a mutable version of the {@link MySqlSplit}. It has a modifiable
 * split status and split offset (i.e. BinlogPosition).
 */
public final class MySqlSplitState extends MySqlSplit {

    private BinlogOffset lowWatermarkState;
    private BinlogOffset highWatermarkState;
    private BinlogOffset offsetState;
    private boolean snapshotReadFinishedState;

    @Nullable private Map<TableId, SchemaRecord> databaseHistoryState;

    public MySqlSplitState(MySqlSplit split) {
        super(
                split.getSplitKind(),
                split.getTableId(),
                split.getSplitId(),
                split.getSplitBoundaryType(),
                split.getSplitBoundaryStart(),
                split.getSplitBoundaryEnd(),
                split.getLowWatermark(),
                split.getHighWatermark(),
                split.isSnapshotReadFinished(),
                split.getOffset(),
                split.getFinishedSplitsInfo(),
                split.getDatabaseHistory());
        this.lowWatermarkState = split.getLowWatermark();
        this.highWatermarkState = split.getHighWatermark();
        this.offsetState = split.getOffset();
        this.snapshotReadFinishedState = split.isSnapshotReadFinished();
        this.databaseHistoryState =
                split.getDatabaseHistory() == null ? new HashMap<>() : split.getDatabaseHistory();
    }

    public void setLowWatermarkState(BinlogOffset lowWatermarkState) {
        this.lowWatermarkState = lowWatermarkState;
    }

    public void setHighWatermarkState(BinlogOffset highWatermarkState) {
        this.highWatermarkState = highWatermarkState;
    }

    public void setOffsetState(BinlogOffset offsetState) {
        this.offsetState = offsetState;
    }

    public void setSnapshotReadFinishedState(boolean snapshotReadFinishedState) {
        this.snapshotReadFinishedState = snapshotReadFinishedState;
    }

    @Nullable
    public Map<TableId, SchemaRecord> getDatabaseHistoryState() {
        return databaseHistoryState;
    }

    public void recordSchemaHistory(TableId tableId, SchemaRecord latestSchemaChange) {
        this.databaseHistoryState.put(tableId, latestSchemaChange);
    }

    public BinlogOffset getLowWatermarkState() {
        return lowWatermarkState;
    }

    public BinlogOffset getHighWatermarkState() {
        return highWatermarkState;
    }

    @Nullable
    public BinlogOffset getOffsetState() {
        return offsetState;
    }

    public boolean isSnapshotReadFinishedState() {
        return snapshotReadFinishedState;
    }

    /** Use the current split state to create a new MySQLSplit. */
    public MySqlSplit toMySQLSplit() {
        return new MySqlSplit(
                getSplitKind(),
                getTableId(),
                getSplitId(),
                getSplitBoundaryType(),
                getSplitBoundaryStart(),
                getSplitBoundaryEnd(),
                getLowWatermarkState(),
                getHighWatermarkState(),
                isSnapshotReadFinishedState(),
                getOffsetState(),
                getFinishedSplitsInfo(),
                getDatabaseHistory());
    }
}
