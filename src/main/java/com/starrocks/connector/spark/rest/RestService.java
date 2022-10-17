// Modifications Copyright 2021 StarRocks Limited.
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

package com.starrocks.connector.spark.rest;

import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_BENODES;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FILTER_QUERY;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_READ_FIELD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_PASSWORD;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_REQUEST_AUTH_USER;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_DEFAULT;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLET_SIZE_MIN;
import static com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_TABLE_IDENTIFIER;
import static com.starrocks.connector.spark.util.ErrorMessages.CONNECT_FAILED_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.ILLEGAL_ARGUMENT_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.PARSE_NUMBER_FAILED_MESSAGE;
import static com.starrocks.connector.spark.util.ErrorMessages.SHOULD_NOT_HAPPEN_MESSAGE;

import com.google.common.annotations.VisibleForTesting;
import com.starrocks.connector.spark.cfg.ConfigurationOptions;
import com.starrocks.connector.spark.cfg.Settings;
import com.starrocks.connector.spark.cfg.SparkSettings;
import com.starrocks.connector.spark.exception.ConnectedFailedException;
import com.starrocks.connector.spark.exception.StarrocksException;
import com.starrocks.connector.spark.exception.IllegalArgumentException;
import com.starrocks.connector.spark.exception.ShouldNeverHappenException;
import com.starrocks.connector.spark.rest.models.BackendV2;
import com.starrocks.connector.spark.rest.models.QueryPlan;
import com.starrocks.connector.spark.rest.models.Schema;
import com.starrocks.connector.spark.rest.models.Tablet;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Service for communicate with StarRocks FE.
 */
public class RestService implements Serializable {
    public final static int REST_RESPONSE_STATUS_OK = 200;
    private static final String API_PREFIX = "/api";
    private static final String SCHEMA = "_schema";
    private static final String QUERY_PLAN = "_query_plan";

    @Deprecated
    private static final String BACKENDS = "/rest/v1/system?path=//backends";
    private static final String BACKENDS_V2 = "/api/backends?is_alive=true";

    /**
     * send request to StarRocks FE and get response json string.
     *
     * @param cfg     configuration of request
     * @param request {@link HttpRequestBase} real request
     * @param logger  {@link Logger}
     * @return StarRocks FE response in json string
     * @throws ConnectedFailedException throw when cannot connect to StarRocks FE
     */
    private static String send(Settings cfg, HttpRequestBase request, Logger logger) throws
            ConnectedFailedException {
        int connectTimeout = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS,
                ConfigurationOptions.STARROCKS_REQUEST_CONNECT_TIMEOUT_MS_DEFAULT);
        int socketTimeout = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS,
                ConfigurationOptions.STARROCKS_REQUEST_READ_TIMEOUT_MS_DEFAULT);
        int retries = cfg.getIntegerProperty(ConfigurationOptions.STARROCKS_REQUEST_RETRIES,
                ConfigurationOptions.STARROCKS_REQUEST_RETRIES_DEFAULT);
        logger.trace("connect timeout set to '{}'. socket timeout set to '{}'. retries set to '{}'.",
                connectTimeout, socketTimeout, retries);

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectTimeout)
                .setSocketTimeout(socketTimeout)
                .build();

        request.setConfig(requestConfig);

        String user = cfg.getProperty(STARROCKS_REQUEST_AUTH_USER, "");
        String password = cfg.getProperty(STARROCKS_REQUEST_AUTH_PASSWORD, "");

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY,
                new UsernamePasswordCredentials(user, password));
        HttpClientContext context = HttpClientContext.create();
        context.setCredentialsProvider(credentialsProvider);
        logger.info("Send request to StarRocks FE '{}' with user '{}'.", request.getURI(), user);

        IOException ex = null;
        int statusCode = -1;

        for (int attempt = 0; attempt < retries; attempt++) {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            logger.debug("Attempt {} to request {}.", attempt, request.getURI());
            try {
                CloseableHttpResponse response = httpClient.execute(request, context);
                statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    logger.warn("Failed to get response from StarRocks FE {}, http code is {}",
                            request.getURI(), statusCode);
                    continue;
                }
                String res = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                logger.trace("Success get response from StarRocks FE: {}, response is: {}.",
                        request.getURI(), res);
                return res;
            } catch (IOException e) {
                ex = e;
                logger.warn(CONNECT_FAILED_MESSAGE, request.getURI(), e);
            }
        }

        logger.error(CONNECT_FAILED_MESSAGE, request.getURI(), ex);
        throw new ConnectedFailedException(request.getURI().toString(), statusCode, ex);
    }

    /**
     * parse table identifier to array.
     *
     * @param tableIdentifier table identifier string
     * @param logger          {@link Logger}
     * @return first element is db name, second element is table name
     * @throws IllegalArgumentException table identifier is illegal
     */
    @VisibleForTesting
    static String[] parseIdentifier(String tableIdentifier, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse identifier '{}'.", tableIdentifier);
        if (StringUtils.isEmpty(tableIdentifier)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        String[] identifier = tableIdentifier.split("\\.");
        if (identifier.length != 2) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "table.identifier", tableIdentifier);
            throw new IllegalArgumentException("table.identifier", tableIdentifier);
        }
        return identifier;
    }

    /**
     * choice a StarRocks FE node to request.
     *
     * @param feNodes StarRocks FE node list, separate be comma
     * @param logger  slf4j logger
     * @return the chosen one StarRocks FE node
     * @throws IllegalArgumentException fe nodes is illegal
     */
    @VisibleForTesting
    public static String randomEndpoint(String feNodes, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse nodes '{}'.", feNodes);
        if (StringUtils.isEmpty(feNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "fenodes", feNodes);
            throw new IllegalArgumentException("fenodes", feNodes);
        }
        List<String> nodes = Arrays.asList(feNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    public static String randomBeNode(String beNodes, Logger logger) throws IllegalArgumentException {
        logger.trace("Parse nodes '{}'.", beNodes);
        if (StringUtils.isEmpty(beNodes)) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "benodes", beNodes);
            throw new IllegalArgumentException("benodes", beNodes);
        }
        List<String> nodes = Arrays.asList(beNodes.split(","));
        Collections.shuffle(nodes);
        return nodes.get(0).trim();
    }

    /**
     * get a valid URI to connect StarRocks FE.
     *
     * @param cfg    configuration of request
     * @param logger {@link Logger}
     * @return uri string
     * @throws IllegalArgumentException throw when configuration is illegal
     */
    @VisibleForTesting
    static String getUriStr(Settings cfg, Logger logger) throws IllegalArgumentException {
        String[] identifier = parseIdentifier(cfg.getProperty(STARROCKS_TABLE_IDENTIFIER), logger);
        return "http://" +
                randomEndpoint(cfg.getProperty(STARROCKS_FENODES), logger) + API_PREFIX +
                //randomBeNode(cfg.getProperty(STARROCKS_BENODES), logger) + API_PREFIX +
                "/" + identifier[0] +
                "/" + identifier[1] +
                "/";
    }

    /**
     * discover StarRocks table schema from StarRocks FE.
     *
     * @param cfg    configuration of request
     * @param logger slf4j logger
     * @return StarRocks table schema
     * @throws StarrocksException throw when discover failed
     */
    public static Schema getSchema(Settings cfg, Logger logger)
            throws StarrocksException {
        logger.trace("Finding schema.");
        HttpGet httpGet = new HttpGet(getUriStr(cfg, logger) + SCHEMA);
        String response = send(cfg, httpGet, logger);
        logger.debug("Find schema response is '{}'.", response);
        return parseSchema(response, logger);
    }

    /**
     * translate StarRocks FE response to inner {@link Schema} struct.
     *
     * @param response StarRocks FE response
     * @param logger   {@link Logger}
     * @return inner {@link Schema} struct
     * @throws StarrocksException throw when translate failed
     */
    @VisibleForTesting
    public static Schema parseSchema(String response, Logger logger) throws StarrocksException {
        logger.trace("Parse response '{}' to schema.", response);
        ObjectMapper mapper = new ObjectMapper();
        Schema schema;
        try {
            schema = mapper.readValue(response, Schema.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        }

        if (schema == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (schema.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + schema.getStatus();
            logger.error(errMsg);
            throw new StarrocksException(errMsg);
        }
        logger.debug("Parsing schema result is '{}'.", schema);
        return schema;
    }

    /**
     * find StarRocks RDD partitions from StarRocks FE.
     *
     * @param cfg    configuration of request
     * @param logger {@link Logger}
     * @return an list of StarRocks RDD partitions
     * @throws StarrocksException throw when find partition failed
     */
    public static List<PartitionDefinition> findPartitions(Settings cfg, Logger logger) throws StarrocksException {
        String[] tableIdentifiers = parseIdentifier(cfg.getProperty(STARROCKS_TABLE_IDENTIFIER), logger);
        String sql = "select " + cfg.getProperty(STARROCKS_READ_FIELD, "*") +
                " from `" + tableIdentifiers[0] + "`.`" + tableIdentifiers[1] + "`";
        if (!StringUtils.isEmpty(cfg.getProperty(STARROCKS_FILTER_QUERY))) {
            sql += " where " + cfg.getProperty(STARROCKS_FILTER_QUERY);
        }
        logger.debug("Query SQL Sending to StarRocks FE is: '{}'.", sql);

        HttpPost httpPost = new HttpPost(getUriStr(cfg, logger) + QUERY_PLAN);
        String entity = "{\"sql\": \"" + sql + "\"}";
        logger.debug("Post body Sending to StarRocks FE is: '{}'.", entity);
        StringEntity stringEntity = new StringEntity(entity, StandardCharsets.UTF_8);
        stringEntity.setContentEncoding("UTF-8");
        stringEntity.setContentType("application/json");
        httpPost.setEntity(stringEntity);

        String resStr = send(cfg, httpPost, logger);
        logger.debug("Find partition response is '{}'.", resStr);
        QueryPlan queryPlan = getQueryPlan(resStr, logger);
        Map<String, List<Long>> be2Tablets = selectBeForTablet(queryPlan, logger);
        return tabletsMapToPartition(
                cfg,
                be2Tablets,
                queryPlan.getOpaqued_query_plan(),
                tableIdentifiers[0],
                tableIdentifiers[1],
                logger);
    }

    /**
     * translate StarRocks FE response string to inner {@link QueryPlan} struct.
     *
     * @param response StarRocks FE response string
     * @param logger   {@link Logger}
     * @return inner {@link QueryPlan} struct
     * @throws StarrocksException throw when translate failed.
     */
    @VisibleForTesting
    static QueryPlan getQueryPlan(String response, Logger logger) throws StarrocksException {
        ObjectMapper mapper = new ObjectMapper();
        QueryPlan queryPlan;
        try {
            queryPlan = mapper.readValue(response, QueryPlan.class);
        } catch (JsonParseException e) {
            String errMsg = "StarRocks FE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (JsonMappingException e) {
            String errMsg = "StarRocks FE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks FE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        }

        if (queryPlan == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }

        if (queryPlan.getStatus() != REST_RESPONSE_STATUS_OK) {
            String errMsg = "StarRocks FE's response is not OK, status is " + queryPlan.getStatus();
            logger.error(errMsg);
            throw new StarrocksException(errMsg);
        }
        logger.debug("Parsing partition result is '{}'.", queryPlan);
        return queryPlan;
    }

    /**
     * select which StarRocks BE to get tablet data.
     *
     * @param queryPlan {@link QueryPlan} translated from StarRocks FE response
     * @param logger    {@link Logger}
     * @return BE to tablets {@link Map}
     * @throws StarrocksException throw when select failed.
     */
    @VisibleForTesting
    static Map<String, List<Long>> selectBeForTablet(QueryPlan queryPlan, Logger logger) throws StarrocksException {
        Map<String, List<Long>> be2Tablets = new HashMap<>();
        for (Map.Entry<String, Tablet> part : queryPlan.getPartitions().entrySet()) {
            logger.debug("Parse tablet info: '{}'.", part);
            long tabletId;
            try {
                tabletId = Long.parseLong(part.getKey());
            } catch (NumberFormatException e) {
                String errMsg = "Parse tablet id '" + part.getKey() + "' to long failed.";
                logger.error(errMsg, e);
                throw new StarrocksException(errMsg, e);
            }
            String target = null;
            int tabletCount = Integer.MAX_VALUE;
            for (String candidate : part.getValue().getRoutings()) {
                logger.trace("Evaluate StarRocks BE '{}' to tablet '{}'.", candidate, tabletId);
                if (!be2Tablets.containsKey(candidate)) {
                    logger.debug("Choice a new StarRocks BE '{}' for tablet '{}'.", candidate, tabletId);
                    List<Long> tablets = new ArrayList<>();
                    be2Tablets.put(candidate, tablets);
                    target = candidate;
                    break;
                } else {
                    if (be2Tablets.get(candidate).size() < tabletCount) {
                        target = candidate;
                        tabletCount = be2Tablets.get(candidate).size();
                        logger.debug("Current candidate StarRocks BE to tablet '{}' is '{}' with tablet count {}.",
                                tabletId, target, tabletCount);
                    }
                }
            }
            if (target == null) {
                String errMsg = "Cannot choice StarRocks BE for tablet " + tabletId;
                logger.error(errMsg);
                throw new StarrocksException(errMsg);
            }

            logger.debug("Choice StarRocks BE '{}' for tablet '{}'.", target, tabletId);
            be2Tablets.get(target).add(tabletId);
        }
        return be2Tablets;
    }

    /**
     * tablet count limit for one StarRocks RDD partition
     *
     * @param cfg    configuration of request
     * @param logger {@link Logger}
     * @return tablet count limit
     */
    @VisibleForTesting
    static int tabletCountLimitForOnePartition(Settings cfg, Logger logger) {
        int tabletsSize = STARROCKS_TABLET_SIZE_DEFAULT;
        if (cfg.getProperty(STARROCKS_TABLET_SIZE) != null) {
            try {
                tabletsSize = Integer.parseInt(cfg.getProperty(STARROCKS_TABLET_SIZE));
            } catch (NumberFormatException e) {
                logger.warn(PARSE_NUMBER_FAILED_MESSAGE, STARROCKS_TABLET_SIZE, cfg.getProperty(STARROCKS_TABLET_SIZE));
            }
        }
        if (tabletsSize < STARROCKS_TABLET_SIZE_MIN) {
            logger.warn("{} is less than {}, set to default value {}.",
                    STARROCKS_TABLET_SIZE, STARROCKS_TABLET_SIZE_MIN, STARROCKS_TABLET_SIZE_MIN);
            tabletsSize = STARROCKS_TABLET_SIZE_MIN;
        }
        logger.debug("Tablet size is set to {}.", tabletsSize);
        return tabletsSize;
    }

    /**
     * translate BE tablets map to StarRocks RDD partition.
     *
     * @param cfg              configuration of request
     * @param be2Tablets       BE to tablets {@link Map}
     * @param opaquedQueryPlan StarRocks BE execute plan getting from StarRocks FE
     * @param database         database name of StarRocks table
     * @param table            table name of StarRocks table
     * @param logger           {@link Logger}
     * @return Starrocks RDD partition {@link List}
     * @throws IllegalArgumentException throw when translate failed
     */
    @VisibleForTesting
    static List<PartitionDefinition> tabletsMapToPartition(Settings cfg, Map<String, List<Long>> be2Tablets,
                                                           String opaquedQueryPlan, String database, String table,
                                                           Logger logger)
            throws IllegalArgumentException {
        int tabletsSize = tabletCountLimitForOnePartition(cfg, logger);
        List<PartitionDefinition> partitions = new ArrayList<>();
        for (Map.Entry<String, List<Long>> beInfo : be2Tablets.entrySet()) {
            logger.debug("Generate partition with beInfo: '{}'.", beInfo);
            HashSet<Long> tabletSet = new HashSet<>(beInfo.getValue());
            beInfo.getValue().clear();
            beInfo.getValue().addAll(tabletSet);
            int first = 0;
            while (first < beInfo.getValue().size()) {
                Set<Long> partitionTablets = new HashSet<>(beInfo.getValue().subList(
                        first, Math.min(beInfo.getValue().size(), first + tabletsSize)));
                first = first + tabletsSize;
                PartitionDefinition partitionDefinition =
                        new PartitionDefinition(database, table, cfg,
                                beInfo.getKey(), partitionTablets, opaquedQueryPlan);
                logger.debug("Generate one PartitionDefinition '{}'.", partitionDefinition);
                partitions.add(partitionDefinition);
            }
        }
        return partitions;
    }

    public static String randomBackendV3(SparkSettings sparkSettings, Logger logger) throws StarrocksException {
        String beNodes = sparkSettings.getProperty(STARROCKS_BENODES);
        String beNode = randomBeNode(beNodes, logger);
        return beNode;
    }

    /**
     * choice a StarRocks BE node to request.
     * @param logger slf4j logger
     * @return the chosen one StarRocks BE node
     * @throws IllegalArgumentException BE nodes is illegal
     */
    @VisibleForTesting
    public static String randomBackendV2(SparkSettings sparkSettings, Logger logger) throws StarrocksException {
        String feNodes = sparkSettings.getProperty(STARROCKS_FENODES);
        String feNode = randomEndpoint(feNodes, logger);
        String beUrl =   String.format("http://%s" + BACKENDS_V2, feNode);
        HttpGet httpGet = new HttpGet(beUrl);
        String response = send(sparkSettings, httpGet, logger);
        logger.info("Backend Info:{}", response);
        List<BackendV2.BackendRowV2> backends = parseBackendV2(response, logger);
        logger.trace("Parse beNodes '{}'.", backends);
        if (backends == null || backends.isEmpty()) {
            logger.error(ILLEGAL_ARGUMENT_MESSAGE, "beNodes", backends);
            throw new IllegalArgumentException("beNodes", String.valueOf(backends));
        }
        Collections.shuffle(backends);
        BackendV2.BackendRowV2 backend = backends.get(0);
        return backend.getIp() + ":" + backend.getHttpPort();
    }

    static List<BackendV2.BackendRowV2> parseBackendV2(String response, Logger logger) throws StarrocksException {
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
        BackendV2 backend;
        try {
            backend = mapper.readValue(response, BackendV2.class);
        } catch (com.fasterxml.jackson.core.JsonParseException e) {
            String errMsg = "StarRocks BE's response is not a json. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (com.fasterxml.jackson.databind.JsonMappingException e) {
            String errMsg = "StarRocks BE's response cannot map to schema. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        } catch (IOException e) {
            String errMsg = "Parse StarRocks BE's response to json failed. res: " + response;
            logger.error(errMsg, e);
            throw new StarrocksException(errMsg, e);
        }

        if (backend == null) {
            logger.error(SHOULD_NOT_HAPPEN_MESSAGE);
            throw new ShouldNeverHappenException();
        }
        List<BackendV2.BackendRowV2> backendRows = backend.getBackends();
        logger.debug("Parsing schema result is '{}'.", backendRows);
        return backendRows;
    }

}
