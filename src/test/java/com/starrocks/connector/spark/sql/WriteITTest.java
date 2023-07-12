// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.connector.spark.sql;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class WriteITTest extends ITTestBase {

// StarRocks table
//    CREATE TABLE `score_board` (
//    `id` int(11) NOT NULL COMMENT "",
//    `name` varchar(65533) NULL DEFAULT "" COMMENT "",
//    `score` int(11) NOT NULL DEFAULT "0" COMMENT ""
//    ) ENGINE=OLAP
//    PRIMARY KEY(`id`)
//    COMMENT "OLAP"
//    DISTRIBUTED BY HASH(`id`)
//    PROPERTIES (
//        "replication_num" = "1"
//    );

    private static final String DB = "starrocks";
    private static final String TABLE = "score_board";
    private static final String TABLE_ID = DB + "." + TABLE;

    @Test
    public void testDataFrame() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testSql() {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testSql")
                .getOrCreate();

        String ddl = String.format("CREATE TABLE sink \n" +
                " USING starrocks\n" +
                "OPTIONS(\n" +
                "  \"starrocks.table.identifier\"=\"%s\",\n" +
                "  \"starrocks.fe.http.url\"=\"%s\",\n" +
                "  \"starrocks.fe.jdbc.url\"=\"%s\",\n" +
                "  \"user\"=\"%s\",\n" +
                "  \"password\"=\"%s\"\n" +
                ")", TABLE_ID, FE_HTTP, FE_JDBC, USER, PASSWORD);
        spark.sql(ddl);
        spark.sql("INSERT INTO sink VALUES (1, \"2\", 3), (2, \"3\", 4)");

        spark.stop();
    }

    @Test
    public void testDefaultConfiguration() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testCsvConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "|");
        options.put("starrocks.write.properties.column_separator", ",");
        testConfigurationBase(options);
    }

    @Test
    public void testJsonConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.properties.format", "json");
        testConfigurationBase(options);
    }

    @Test
    public void testTransactionConfiguration() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("starrocks.write.enable.transaction-stream-load", "false");
        testConfigurationBase(options);
    }

    private void testConfigurationBase(Map<String, String> customOptions) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = Arrays.asList(
                RowFactory.create(1, "2", 3),
                RowFactory.create(2, "3", 4)
        );

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.request.retries", "4");
        options.put("starrocks.request.connect.timeout.ms", "40000");
        options.put("starrocks.request.read.timeout.ms", "5000");
        options.put("starrocks.columns", "id,name,score");
        options.put("starrocks.write.label.prefix", "spark-connector-");
        options.put("starrocks.write.wait-for-continue.timeout.ms", "10000");
        options.put("starrocks.write.chunk.limit", "100k");
        options.put("starrocks.write.scan-frequency.ms", "100");
        options.put("starrocks.write.enable.transaction-stream-load", "true");
        options.put("starrocks.write.buffer.size", "12k");
        options.put("starrocks.write.flush.interval.ms", "3000");
        options.put("starrocks.write.max.retries", "2");
        options.put("starrocks.write.retry.interval.ms", "1000");
        options.put("starrocks.write.properties.format", "csv");
        options.put("starrocks.write.properties.row_delimiter", "\n");
        options.put("starrocks.write.properties.column_separator", "\t");
        options.putAll(customOptions);

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }

    @Test
    public void testStructuredStreaming() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testStructuredStreaming")
                .getOrCreate();

        StructType schema = new StructType(new StructField[] {
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });
        Dataset<Row> df = spark.readStream()
                .option("sep", ",")
                .schema(schema)
                .format("csv")
                .load("src/test/resources/simple_write_csv/");

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);

        FileUtils.deleteDirectory(new File("checkpoint"));
        StreamingQuery query = df.writeStream()
                .format("starrocks")
                .outputMode(OutputMode.Append())
                .options(options)
                .option("checkpointLocation", "checkpoint")
                .start();

        query.awaitTermination();

        spark.stop();
    }

    @Test
    public void testDataFramePartition() {
        testDataFramePartitionBase("5", null);
    }

    @Test
    public void testDataFramePartitionColumns() {
        testDataFramePartitionBase("10", "name,score");
    }

    private void testDataFramePartitionBase(String numPartitions, String partitionColumns) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[1]")
                .appName("testDataFrame")
                .getOrCreate();

        List<Row> data = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            data.add(RowFactory.create(i, String.valueOf(i + 1), i + 2));
        }

        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("score", DataTypes.IntegerType, false, Metadata.empty())
        });

        Dataset<Row> df = spark.createDataFrame(data, schema);

        Map<String, String> options = new HashMap<>();
        options.put("starrocks.fe.http.url", FE_HTTP);
        options.put("starrocks.fe.jdbc.url", FE_JDBC);
        options.put("starrocks.table.identifier", TABLE_ID);
        options.put("starrocks.user", USER);
        options.put("starrocks.password", PASSWORD);
        options.put("starrocks.write.num.partitions", numPartitions);
        if (partitionColumns != null) {
            options.put("starrocks.write.partition.columns", partitionColumns);
        }

        df.write().format("starrocks")
                .mode(SaveMode.Append)
                .options(options)
                .save();

        spark.stop();
    }
}
