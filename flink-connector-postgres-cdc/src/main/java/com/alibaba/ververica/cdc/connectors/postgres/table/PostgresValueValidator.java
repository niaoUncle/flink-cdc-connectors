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

package com.alibaba.ververica.cdc.connectors.postgres.table;

import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

import com.alibaba.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;

/** The {@link RowDataDebeziumDeserializeSchema.ValueValidator} for Postgres connector. */
public final class PostgresValueValidator
        implements RowDataDebeziumDeserializeSchema.ValueValidator {
    private static final long serialVersionUID = -1870679469578028765L;

    private static final String REPLICA_IDENTITY_EXCEPTION =
            "The \"before\" field of UPDATE/DELETE message is null, "
                    + "please check the Postgres table has been set REPLICA IDENTITY to FULL level. "
                    + "You can update the setting by running the command in Postgres 'ALTER TABLE %s REPLICA IDENTITY FULL'. "
                    + "Please see more in Debezium documentation: https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity";

    private final String schemaTable;

    public PostgresValueValidator(String schema, String table) {
        this.schemaTable = schema + "." + table;
    }

    @Override
    public void validate(RowData rowData, RowKind rowKind) throws Exception {
        if (rowData == null) {
            throw new IllegalStateException(String.format(REPLICA_IDENTITY_EXCEPTION, schemaTable));
        }
    }
}
