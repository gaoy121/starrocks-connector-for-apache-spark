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

package com.starrocks.connector.spark;

import com.google.common.cache.*;
import com.starrocks.connector.spark.cfg.SparkSettings;
import com.starrocks.connector.spark.exception.StarrocksException;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * a cached streamload client for each partition
 */
public class CachedStarRocksStreamLoadClient {
    private static final long cacheExpireTimeout = 30 * 60;
    private static LoadingCache<SparkSettings, StarRocksStreamLoad> starRocksStreamLoadLoadingCache;

    static {
        starRocksStreamLoadLoadingCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheExpireTimeout, TimeUnit.SECONDS)
                .removalListener(new RemovalListener<Object, Object>() {
                    @Override
                    public void onRemoval(RemovalNotification<Object, Object> removalNotification) {
                        //do nothing
                    }
                })
                .build(
                        new CacheLoader<SparkSettings, StarRocksStreamLoad>() {
                            @Override
                            public StarRocksStreamLoad load(SparkSettings sparkSettings) throws IOException, StarrocksException {
                                StarRocksStreamLoad starRocksStreamLoad = new StarRocksStreamLoad(sparkSettings);
                                return starRocksStreamLoad;
                            }
                        }
                );
    }

    public static StarRocksStreamLoad getOrCreate(SparkSettings settings) throws ExecutionException {
        StarRocksStreamLoad starRocksStreamLoad = starRocksStreamLoadLoadingCache.get(settings);
        return starRocksStreamLoad;
    }
}
