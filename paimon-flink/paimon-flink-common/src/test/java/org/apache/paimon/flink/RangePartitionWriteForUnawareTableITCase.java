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

package org.apache.paimon.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.operation.AppendOnlyFileStoreScan;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.DataSplit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Test case for append-only managed unaware-bucket table. */
public class RangePartitionWriteForUnawareTableITCase extends CatalogITCaseBase {

    private static final int INPUT_ROW_NUMBER = 1000;

    private static final int SINK_PARALLELISM = 10;

    @Test
    public void testRangePartition() {
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        Row[] inputRowsArray = inputRows.toArray(new Row[0]);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1') */ SELECT * FROM source");
        List<Row> result = batchSql("SELECT * FROM test_table");
        assertThat(result.size()).isEqualTo(INPUT_ROW_NUMBER);
        assertThat(result).containsExactlyInAnyOrder(inputRowsArray);
    }

    @Test
    public void testRangePartitionDataDistribution() throws Exception {
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1') */ SELECT * FROM source");

        FileStoreTable testStoreTable = paimonTable("test_table");
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        List<Tuple2<Integer, Integer>> minMaxOfEachFile = new ArrayList<>();
        for(ManifestEntry file : files){
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .build();
            final AtomicInteger min = new AtomicInteger(Integer.MAX_VALUE);
            final AtomicInteger max = new AtomicInteger(Integer.MIN_VALUE);
            testStoreTable.newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                int result = internalRow.getInt(0);
                                min.set(Math.min(min.get(), result));
                                max.set(Math.max(max.get(), result));
                            });
            minMaxOfEachFile.add(Tuple2.of(min.get(), max.get()));
        }
        minMaxOfEachFile.sort(Comparator.comparing(o -> o.f0));
        Tuple2<Integer, Integer> preResult = minMaxOfEachFile.get(0);
        for(int index = 1; index < minMaxOfEachFile.size(); ++index){
            Tuple2<Integer, Integer> currentResult = minMaxOfEachFile.get(index);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(0);
            assertThat(currentResult.f1).isLessThanOrEqualTo(INPUT_ROW_NUMBER);
            assertThat(currentResult.f0).isGreaterThanOrEqualTo(preResult.f1);
        }
    }


    @Test
    public void testRangePartitionAndSortWithOrderStrategy() {
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        Row[] inputRowsArray = inputRows.toArray(new Row[0]);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1', 'sink.cluster.sort' = 'true', 'sink.cluster.strategy' = 'order') */ SELECT * FROM source");
        List<Row> result = batchSql("SELECT * FROM test_table");
        assertThat(result.size()).isEqualTo(INPUT_ROW_NUMBER);
        assertThat(result).containsExactlyInAnyOrder(inputRowsArray);
    }

    @Test
    public void testRangePartitionAndSortWithOrderStrategyDataDistribution() throws Exception{
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1', 'sink.cluster.sort' = 'true', 'sink.cluster.strategy' = 'order') */ SELECT * FROM source");

        FileStoreTable testStoreTable = paimonTable("test_table");
        List<ManifestEntry> files = testStoreTable.store().newScan().plan().files();
        for(ManifestEntry file : files){
            DataSplit dataSplit =
                    DataSplit.builder()
                            .withPartition(file.partition())
                            .withBucket(file.bucket())
                            .withDataFiles(Collections.singletonList(file.file()))
                            .build();
            final AtomicInteger i = new AtomicInteger(Integer.MIN_VALUE);
            testStoreTable.newReadBuilder()
                    .newRead()
                    .createReader(dataSplit)
                    .forEachRemaining(
                            internalRow -> {
                                Integer current = internalRow.getInt(0);
                                Assertions.assertThat(current).isGreaterThanOrEqualTo(i.get());
                                i.set(current);
                            });
        }
    }

    @Test
    public void testRangePartitionAndSortWithZOrderStrategy() throws Exception{
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        Row[] inputRowsArray = inputRows.toArray(new Row[0]);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1,col2', 'sink.cluster.sort' = 'true', 'sink.cluster.strategy' = 'zorder') */ SELECT * FROM source");
        FileStoreTable fileStoreTable = paimonTable("test_table");
        PredicateBuilder predicateBuilder = new PredicateBuilder(fileStoreTable.rowType());
        Predicate predicate = predicateBuilder.between(1, 100, 200);
        ((AppendOnlyFileStoreScan) fileStoreTable.store().newScan()).withFilter(predicate)
                .plan()
                .files();
    }



    @Test
    public void testRangePartitionAndSortWithZOrderStrategyDataDistribution() {
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        Row[] inputRowsArray = inputRows.toArray(new Row[0]);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1,col2', 'sink.cluster.sort' = 'true', 'sink.cluster.strategy' = 'zorder') */ SELECT * FROM source");
        List<Row> result = batchSql("SELECT * FROM test_table");
        assertThat(result.size()).isEqualTo(INPUT_ROW_NUMBER);
        assertThat(result).containsExactlyInAnyOrder(inputRowsArray);
    }


    @Test
    public void testRangePartitionAndSortWithHilbertStrategy() {
        List<Row> inputRows = generateInputRows();
        String id = TestValuesTableFactory.registerData(inputRows);
        Row[] inputRowsArray = inputRows.toArray(new Row[0]);
        batchSql("CREATE TEMPORARY TABLE source (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('connector'='values', 'bounded'='true', 'data-id'='%s')", id);
        batchSql("INSERT INTO test_table /*+ OPTIONS('sink.cluster.key' = 'col1,col2', 'sink.cluster.sort' = 'true', 'sink.cluster.strategy' = 'hilbert') */ SELECT * FROM source");
        List<Row> result = batchSql("SELECT * FROM test_table");
        assertThat(result.size()).isEqualTo(INPUT_ROW_NUMBER);
        assertThat(result).containsExactlyInAnyOrder(inputRowsArray);
    }

    private List<Row> generateInputRows() {
        List<Row> inputRows = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < INPUT_ROW_NUMBER; i++) {
            inputRows.add(Row.ofKind(RowKind.INSERT,
                    random.nextInt(INPUT_ROW_NUMBER + 1),
                    random.nextInt(INPUT_ROW_NUMBER + 1),
                    random.nextInt(INPUT_ROW_NUMBER + 1),
                    random.nextInt(INPUT_ROW_NUMBER + 1)));
        }
        return inputRows;
    }

    @Override
    protected void setParallelism(int parallelism) {
        tEnv.getConfig()
                .set(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, SINK_PARALLELISM);
    }

    @Override
    protected List<String> ddl() {
        return Collections.singletonList("CREATE TABLE IF NOT EXISTS test_table (col1 INT, col2 INT, col3 INT, col4 INT) WITH ('sink.parallelism' = '10')");
    }
}
