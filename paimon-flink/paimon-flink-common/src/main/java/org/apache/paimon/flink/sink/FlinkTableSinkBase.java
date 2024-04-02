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

package org.apache.paimon.flink.sink;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.LogChangelogMode;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.PaimonDataStreamSinkProvider;
import org.apache.paimon.flink.log.LogSinkProvider;
import org.apache.paimon.flink.log.LogStoreTableFactory;
import org.apache.paimon.flink.sorter.TableSorter;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.paimon.CoreOptions.CHANGELOG_PRODUCER;
import static org.apache.paimon.CoreOptions.LOG_CHANGELOG_MODE;
import static org.apache.paimon.CoreOptions.MERGE_ENGINE;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTER_KEY;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTER_SORT;
import static org.apache.paimon.flink.FlinkConnectorOptions.CLUSTER_STRATEGY;
import static org.apache.paimon.table.BucketMode.UNAWARE;
import static org.apache.paimon.utils.Preconditions.checkState;

/** Table sink to create sink. */
public abstract class FlinkTableSinkBase
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    protected final ObjectIdentifier tableIdentifier;
    protected final DynamicTableFactory.Context context;
    @Nullable protected final LogStoreTableFactory logStoreTableFactory;

    protected final Table table;

    protected Map<String, String> staticPartitions = new HashMap<>();
    protected boolean overwrite = false;

    public FlinkTableSinkBase(
            ObjectIdentifier tableIdentifier,
            Table table,
            DynamicTableFactory.Context context,
            @Nullable LogStoreTableFactory logStoreTableFactory) {
        this.tableIdentifier = tableIdentifier;
        this.table = table;
        this.context = context;
        this.logStoreTableFactory = logStoreTableFactory;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (table.primaryKeys().isEmpty()) {
            // Don't check this, for example, only inserts are available from the database, but the
            // plan phase contains all changelogs
            return requestedMode;
        } else {
            Options options = Options.fromMap(table.options());
            if (options.get(CHANGELOG_PRODUCER) == ChangelogProducer.INPUT) {
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.AGGREGATE) {
                return requestedMode;
            }

            if (options.get(MERGE_ENGINE) == MergeEngine.PARTIAL_UPDATE
                    && new CoreOptions(options).definedAggFunc()) {
                return requestedMode;
            }

            if (options.get(LOG_CHANGELOG_MODE) == LogChangelogMode.ALL) {
                return requestedMode;
            }

            // with primary key, default sink upsert
            ChangelogMode.Builder builder = ChangelogMode.newBuilder();
            for (RowKind kind : requestedMode.getContainedKinds()) {
                if (kind != RowKind.UPDATE_BEFORE) {
                    builder.addContainedKind(kind);
                }
            }
            return builder.build();
        }
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        if (overwrite && !context.isBounded()) {
            throw new UnsupportedOperationException(
                    "Paimon doesn't support streaming INSERT OVERWRITE.");
        }

        LogSinkProvider logSinkProvider = null;
        if (logStoreTableFactory != null) {
            logSinkProvider = logStoreTableFactory.createSinkProvider(this.context, context);
        }

        Options conf = Options.fromMap(table.options());
        // Do not sink to log store when overwrite mode
        final LogSinkFunction logSinkFunction =
                overwrite ? null : (logSinkProvider == null ? null : logSinkProvider.createSink());
        FileStoreTable fileStoreTable = (FileStoreTable) table;
        return new PaimonDataStreamSinkProvider(
                (dataStream) ->
                        new FlinkSinkBuilder(fileStoreTable)
                                .withInput(
                                        partitionAndSortDataStreamByRange(
                                                new DataStream<>(
                                                        dataStream.getExecutionEnvironment(),
                                                        dataStream.getTransformation()),
                                                fileStoreTable,
                                                conf))
                                .withLogSinkFunction(logSinkFunction)
                                .withOverwritePartition(overwrite ? staticPartitions : null)
                                .withParallelism(conf.get(FlinkConnectorOptions.SINK_PARALLELISM))
                                .withBoundedInputStream(context.isBounded())
                                .build());
    }

    @Override
    public DynamicTableSink copy() {
        FlinkTableSink copied =
                new FlinkTableSink(tableIdentifier, table, context, logStoreTableFactory);
        copied.staticPartitions = new HashMap<>(staticPartitions);
        copied.overwrite = overwrite;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "PaimonSink";
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        table.partitionKeys()
                .forEach(
                        partitionKey -> {
                            if (partition.containsKey(partitionKey)) {
                                this.staticPartitions.put(
                                        partitionKey, partition.get(partitionKey));
                            }
                        });
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    private DataStream<RowData> partitionAndSortDataStreamByRange(
            DataStream<RowData> input, FileStoreTable fileStoreTable, Options conf) {
        String clusterKeyString = conf.get(CLUSTER_KEY);
        if (clusterKeyString == null) {
            return input;
        } else {
            checkState(
                    fileStoreTable.bucketMode().equals(UNAWARE),
                    "Cluster supports only tables without primary key where 'bucket' is set to '-1'.");
            List<String> clusterKeys = Arrays.asList(clusterKeyString.split(","));
            List<String> fieldNames = fileStoreTable.schema().fieldNames();
            if (!new HashSet<>(fieldNames).containsAll(new HashSet<>(clusterKeys))) {
                throw new RuntimeException(
                        String.format(
                                "Field names %s should contains all cluster keys %s.",
                                fieldNames, clusterKeys));
            }
            String clusterStrategy = conf.get(CLUSTER_STRATEGY);
            TableSorter sorter =
                    TableSorter.getSorter(
                            input.getExecutionEnvironment(),
                            input,
                            fileStoreTable,
                            clusterStrategy,
                            clusterKeys);
            Boolean enableSort = conf.get(CLUSTER_SORT);
            if (enableSort) {
                return sorter.rangePartitionAndSort();
            } else {
                return sorter.rangePartition();
            }
        }
    }
}
