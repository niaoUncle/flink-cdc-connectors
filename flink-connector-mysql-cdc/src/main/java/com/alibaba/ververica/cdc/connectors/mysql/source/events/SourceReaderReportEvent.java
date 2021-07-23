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

package com.alibaba.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.java.tuple.Tuple2;

import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySqlSourceEnumerator;
import com.alibaba.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySqlSourceReader;

import java.util.ArrayList;

/**
 * The {@link SourceEvent} that {@link MySqlSourceReader} sends to {@link MySqlSourceEnumerator} to
 * notify the snapshot split has read finished.
 */
public class SourceReaderReportEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final ArrayList<Tuple2<String, BinlogOffset>> finishedSplits;

    public SourceReaderReportEvent(ArrayList<Tuple2<String, BinlogOffset>> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }

    public ArrayList<Tuple2<String, BinlogOffset>> getFinishedSplits() {
        return finishedSplits;
    }
}
