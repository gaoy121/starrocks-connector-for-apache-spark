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

package com.starrocks.connector.spark.sql.connect;

import com.starrocks.connector.spark.exception.StarrocksException;
import com.starrocks.connector.spark.sql.conf.StarRocksConfig;
import com.starrocks.connector.spark.sql.schema.StarRocksField;
import com.starrocks.connector.spark.sql.schema.StarRocksSchema;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StarRocksConnector {

    public static StarRocksSchema getSchema(StarRocksConfig config) {

        List<Map<String, String>> columnValues = extractColumnValuesBySql(
                config.getFeJdbcUrl(),
                config.getUsername(),
                config.getPassword(),
                config.getDatabase(),
                config.getTable()
        );

        List<StarRocksField> pks = new ArrayList<>();
        List<StarRocksField> columns = new ArrayList<>();
        for (Map<String, String> columnValue : columnValues) {
            StarRocksField field = new StarRocksField(
                    columnValue.get("COLUMN_NAME"),
                    columnValue.get("DATA_TYPE"),
                    Integer.parseInt(columnValue.get("ORDINAL_POSITION")),
                    columnValue.get("COLUMN_SIZE"),
                    columnValue.get("DECIMAL_DIGITS")
            );
            columns.add(field);
            if ("PRI".equals(columnValue.get("COLUMN_KEY"))) {
                pks.add(field);
            }
        }
        columns.sort(Comparator.comparingInt(StarRocksField::getOrdinalPosition));

        return new StarRocksSchema(columns, pks);
    }

    private static final String TABLE_SCHEMA_QUERY =
            "SELECT `COLUMN_NAME`, `ORDINAL_POSITION`, `COLUMN_KEY`, `DATA_TYPE`, `COLUMN_SIZE`, `DECIMAL_DIGITS` " +
                    "FROM `information_schema`.`COLUMNS` WHERE `TABLE_SCHEMA`=? AND `TABLE_NAME`=?;";

    private static List<Map<String, String>> extractColumnValuesBySql(
            String jdbcUrl, String username,  String password, String database, String table) {
        List<Map<String, String>> columnValues = new ArrayList<>();
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement ps = conn.prepareStatement(TABLE_SCHEMA_QUERY)) {
            ps.setObject(1, database);
            ps.setObject(2, table);
            ResultSet rs = ps.executeQuery();
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getString(i));
                }
                columnValues.add(row);
            }
            rs.close();
            if (columnValues.isEmpty()) {
                throw new StarrocksException(String.format("Can't find schema for %s.%s, and " +
                        "please check whether the table exists", database, table));
            }
            return columnValues;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
