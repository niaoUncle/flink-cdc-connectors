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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.connectors.mysql.MySqlTestBase;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory;
import com.alibaba.ververica.cdc.connectors.mysql.debezium.task.context.StatefulTaskContext;
import com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions;
import com.alibaba.ververica.cdc.connectors.mysql.source.assigner.MySqlSnapshotSplitAssigner;
import com.alibaba.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.alibaba.ververica.cdc.connectors.mysql.source.utils.UniqueDatabase;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import com.github.shyiko.mysql.binlog.BinaryLogClient;
import io.debezium.connector.mysql.MySqlConnection;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.ververica.cdc.connectors.mysql.debezium.EmbeddedFlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME;
import static com.alibaba.ververica.cdc.connectors.mysql.source.MySqlSourceOptions.SCAN_OPTIMIZE_INTEGRAL_KEY;
import static com.alibaba.ververica.cdc.connectors.mysql.source.utils.RecordUtils.isWatermarkEvent;
import static org.junit.Assert.assertEquals;

/** Tests for {@link SnapshotSplitReader}. */
@RunWith(Parameterized.class)
public class SnapshotSplitReaderTest extends MySqlTestBase {

    private static final UniqueDatabase customerDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "customer", "mysqluser", "mysqlpw");

    private final boolean useIntegralTypeOptimization;
    private final BinaryLogClient binaryLogClient;
    private final MySqlConnection mySqlConnection;

    @Parameterized.Parameters(name = "useIntegralTypeOptimization: {0}")
    public static Collection<Boolean> parameters() {
        return Arrays.asList(false, true);
    }

    public SnapshotSplitReaderTest(boolean useIntegralTypeOptimization) {
        this.useIntegralTypeOptimization = useIntegralTypeOptimization;
        Configuration configuration = getConfig(new String[] {"customers"});
        this.binaryLogClient = StatefulTaskContext.getBinaryClient(configuration);
        this.mySqlConnection = StatefulTaskContext.getConnection(configuration);
    }

    @BeforeClass
    public static void init() {
        customerDatabase.createAndInitialize();
    }

    @Test
    public void testReadSingleSnapshotSplit() throws Exception {
        Configuration configuration = getConfig(new String[] {"customers"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<MySqlSplit> mySqlSplits = getMySQLSplits(configuration, pkType);

        String[] expected =
                useIntegralTypeOptimization
                        ? new String[] {}
                        : new String[] {
                            "+I[101, user_1, Shanghai, 123567891234]",
                            "+I[102, user_2, Shanghai, 123567891234]",
                            "+I[103, user_3, Shanghai, 123567891234]",
                            "+I[109, user_4, Shanghai, 123567891234]",
                            "+I[110, user_5, Shanghai, 123567891234]",
                            "+I[111, user_6, Shanghai, 123567891234]",
                            "+I[118, user_7, Shanghai, 123567891234]",
                            "+I[121, user_8, Shanghai, 123567891234]",
                            "+I[123, user_9, Shanghai, 123567891234]"
                        };
        List<String> actual = readTableSnapshotSplits(mySqlSplits, configuration, 1, dataType);
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    @Test
    public void testReadAllSnapshotSplitsForOneTable() throws Exception {
        Configuration configuration = getConfig(new String[] {"customers"});
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("address", DataTypes.STRING()),
                        DataTypes.FIELD("phone_number", DataTypes.STRING()));
        final RowType pkType =
                (RowType) DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT())).getLogicalType();
        List<MySqlSplit> mySqlSplits = getMySQLSplits(configuration, pkType);

        String[] expected =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        List<String> actual =
                readTableSnapshotSplits(mySqlSplits, configuration, mySqlSplits.size(), dataType);
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    @Test
    public void testReadAllSplitForTableWithSingleLine() throws Exception {
        Configuration configuration = getConfig(new String[] {"customer_card_single_line"});
        configuration.set(MySqlSourceOptions.SCAN_SPLIT_COLUMN, "card_no");
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        final RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<MySqlSplit> mySqlSplits = getMySQLSplits(configuration, pkType);
        String[] expected = new String[] {"+I[20001, LEVEL_1, user_1, user with level 1]"};
        List<String> actual =
                readTableSnapshotSplits(mySqlSplits, configuration, mySqlSplits.size(), dataType);
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    @Test
    public void testReadAllSnapshotSplitsForTables() throws Exception {
        Configuration configuration =
                getConfig(new String[] {"customer_card", "customer_card_single_line"});
        configuration.set(MySqlSourceOptions.SCAN_SPLIT_COLUMN, "card_no");
        DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                        DataTypes.FIELD("level", DataTypes.STRING()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("note", DataTypes.STRING()));
        RowType pkType =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("card_no", DataTypes.BIGINT()),
                                        DataTypes.FIELD("level", DataTypes.STRING()))
                                .getLogicalType();
        List<MySqlSplit> mySqlSplits = getMySQLSplits(configuration, pkType);

        String[] expected =
                new String[] {
                    "+I[20001, LEVEL_1, user_1, user with level 1]",
                    "+I[20001, LEVEL_4, user_1, user with level 4]",
                    "+I[20002, LEVEL_4, user_2, user with level 4]",
                    "+I[20003, LEVEL_4, user_3, user with level 4]",
                    "+I[20004, LEVEL_1, user_4, user with level 4]",
                    "+I[20004, LEVEL_2, user_4, user with level 4]",
                    "+I[20004, LEVEL_3, user_4, user with level 4]",
                    "+I[20004, LEVEL_4, user_4, user with level 4]",
                    "+I[30006, LEVEL_3, user_5, user with level 3]",
                    "+I[30007, LEVEL_3, user_6, user with level 3]",
                    "+I[30008, LEVEL_3, user_7, user with level 3]",
                    "+I[30009, LEVEL_1, user_8, user with level 3]",
                    "+I[30009, LEVEL_2, user_8, user with level 3]",
                    "+I[30009, LEVEL_3, user_8, user with level 3]",
                    "+I[40001, LEVEL_2, user_9, user with level 2]",
                    "+I[40002, LEVEL_2, user_10, user with level 2]",
                    "+I[40003, LEVEL_2, user_11, user with level 2]",
                    "+I[50001, LEVEL_1, user_12, user with level 1]",
                    "+I[50002, LEVEL_1, user_13, user with level 1]",
                    "+I[50003, LEVEL_1, user_14, user with level 1]",
                };
        List<String> actual =
                readTableSnapshotSplits(mySqlSplits, configuration, mySqlSplits.size(), dataType);
        assertEquals(Arrays.stream(expected).sorted().collect(Collectors.toList()), actual);
    }

    private List<String> readTableSnapshotSplits(
            List<MySqlSplit> mySqlSplits,
            Configuration configuration,
            int scanSplitsNum,
            DataType dataType)
            throws Exception {

        StatefulTaskContext statefulTaskContext =
                new StatefulTaskContext(configuration, binaryLogClient, mySqlConnection);
        SnapshotSplitReader snapshotSplitReader = new SnapshotSplitReader(statefulTaskContext, 0);

        List<SourceRecord> result = new ArrayList<>();
        for (int i = 0; i < scanSplitsNum; i++) {
            MySqlSplit sqlSplit = mySqlSplits.get(i);
            if (snapshotSplitReader.isIdle()) {
                snapshotSplitReader.submitSplit(sqlSplit);
            }
            Iterator<SourceRecord> res;
            while ((res = snapshotSplitReader.pollSplitRecords()) != null) {
                while (res.hasNext()) {
                    SourceRecord sourceRecord = res.next();
                    result.add(sourceRecord);
                }
            }
        }

        if (mySqlConnection != null) {
            mySqlConnection.close();
        }
        if (binaryLogClient != null) {
            binaryLogClient.disconnect();
        }
        return formatResult(result, dataType);
    }

    private List<String> formatResult(List<SourceRecord> records, DataType dataType) {
        final RowType rowType = (RowType) dataType.getLogicalType();
        final TypeInformation<RowData> typeInfo =
                (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(dataType);
        final DebeziumDeserializationSchema<RowData> deserializationSchema =
                new RowDataDebeziumDeserializeSchema(
                        rowType, typeInfo, ((rowData, rowKind) -> {}), ZoneId.of("UTC"));
        SimpleCollector collector = new SimpleCollector();
        RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        records.stream()
                // filter signal event
                .filter(r -> !isWatermarkEvent(r))
                .forEach(
                        r -> {
                            try {
                                deserializationSchema.deserialize(r, collector);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        return collector.list.stream()
                .map(rowRowConverter::toExternal)
                .map(Row::toString)
                .sorted()
                .collect(Collectors.toList());
    }

    private List<MySqlSplit> getMySQLSplits(Configuration configuration, RowType pkType) {
        MySqlSnapshotSplitAssigner assigner =
                new MySqlSnapshotSplitAssigner(
                        configuration, pkType, new ArrayList<>(), new ArrayList<>());
        assigner.open();
        List<MySqlSplit> mySqlSplitList = new ArrayList<>();
        while (true) {
            Optional<MySqlSplit> mySQLSplit = assigner.getNext(null);
            if (mySQLSplit.isPresent()) {
                mySqlSplitList.add(mySQLSplit.get());
            } else {
                break;
            }
        }
        assigner.close();
        return mySqlSplitList;
    }

    private Configuration getConfig(String[] captureTables) {
        Map<String, String> properties = new HashMap<>();
        properties.put("database.server.name", "embedded-test");
        properties.put("database.hostname", MYSQL_CONTAINER.getHost());
        properties.put("database.port", String.valueOf(MYSQL_CONTAINER.getDatabasePort()));
        properties.put("database.user", customerDatabase.getUsername());
        properties.put("database.password", customerDatabase.getPassword());
        properties.put("database.whitelist", customerDatabase.getDatabaseName());
        properties.put("database.history.skip.unparseable.ddl", "true");
        properties.put("server-id-range", "1001, 1002");
        properties.put("database.serverTimezone", ZoneId.of("UTC").toString());
        properties.put("snapshot.mode", "initial");
        properties.put("database.history", EmbeddedFlinkDatabaseHistory.class.getCanonicalName());
        properties.put("database.history.instance.name", DATABASE_HISTORY_INSTANCE_NAME);
        List<String> captureTableIds =
                Arrays.stream(captureTables)
                        .map(tableName -> customerDatabase.getDatabaseName() + "." + tableName)
                        .collect(Collectors.toList());
        properties.put("table.whitelist", String.join(",", captureTableIds));
        properties.put(
                SCAN_OPTIMIZE_INTEGRAL_KEY.key(), String.valueOf(useIntegralTypeOptimization));
        if (useIntegralTypeOptimization) {
            properties.put("scan.split.size", "1000");
            properties.put("scan.fetch.size", "1024");
        } else {
            properties.put("scan.split.size", "10");
            properties.put("scan.fetch.size", "2");
        }
        return Configuration.fromMap(properties);
    }

    static class SimpleCollector implements Collector<RowData> {

        private List<RowData> list = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            list.add(record);
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
