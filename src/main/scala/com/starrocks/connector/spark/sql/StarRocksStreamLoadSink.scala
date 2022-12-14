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

package com.starrocks.connector.spark.sql

import com.fasterxml.jackson.databind.ObjectMapper
import com.starrocks.connector.spark.cfg.{ConfigurationOptions, SparkSettings}
import com.starrocks.connector.spark.{CachedStarRocksStreamLoadClient, StarRocksStreamLoad}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.{Logger, LoggerFactory}
import java.io.IOException
import java.util

import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_FENODES
import com.starrocks.connector.spark.cfg.ConfigurationOptions.STARROCKS_BENODES
import com.starrocks.connector.spark.rest.RestService

import scala.util.control.Breaks

private[sql] class StarRocksStreamLoadSink(sqlContext: SQLContext, settings: SparkSettings) extends Sink with Serializable {

  private val logger: Logger = LoggerFactory.getLogger(classOf[StarRocksStreamLoadSink].getName)
  @volatile private var latestBatchId = -1L
  val maxRowCount: Int = settings.getIntegerProperty(ConfigurationOptions.STARROCKS_SINK_BATCH_SIZE, ConfigurationOptions.SINK_BATCH_SIZE_DEFAULT)
  val maxRetryTimes: Int = settings.getIntegerProperty(ConfigurationOptions.STARROCKS_SINK_MAX_RETRIES, ConfigurationOptions.SINK_MAX_RETRIES_DEFAULT)
  val starRocksStreamLoader: StarRocksStreamLoad = CachedStarRocksStreamLoadClient.getOrCreate(settings)

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logger.info(s"Skipping already committed batch $batchId")
    } else {
      write(data.queryExecution)
      latestBatchId = batchId
    }
  }

  def write(queryExecution: QueryExecution): Unit = {
    queryExecution.toRdd.foreachPartition(iter => {
      val objectMapper = new ObjectMapper()
      val arrayNode = objectMapper.createArrayNode()
      iter.foreach(row => {
        val line: util.List[Object] = new util.ArrayList[Object](maxRowCount)
        for (i <- 0 until row.numFields) {
          val field = row.copy().getUTF8String(i)
          arrayNode.add(objectMapper.readTree(field.toString))
        }
        if (arrayNode.size > maxRowCount - 1) {
          flush
        }
      })
      // flush buffer
      if (!(arrayNode.size <= 0)) {
        flush
      }

      /**
       * flush data to StarRocks and do retry when flush error
       *
       */
      def flush = {
        val loop = new Breaks
        loop.breakable {

          for (i <- 0 to maxRetryTimes) {
            try {
              starRocksStreamLoader.load(arrayNode.toString)
              arrayNode.removeAll()
              loop.break()
            }
            catch {
              case e: Exception =>
                try {
                  logger.debug("Failed to load data on BE: {} node ", starRocksStreamLoader.getLoadUrlStr)
                  //If the current BE node fails to execute Stream Load, randomly switch to other BE nodes and try again
                  //starRocksStreamLoader.setHostPort(RestService.randomBackendV2(settings, logger))
                  starRocksStreamLoader.setHostPort(RestService.randomBackendV3(settings, logger))
                  Thread.sleep(1000 * i)
                } catch {
                  case ex: InterruptedException =>
                    logger.warn("Data that failed to load : " + arrayNode.toString)
                    Thread.currentThread.interrupt()
                    throw new IOException("unable to flush; interrupted while doing another attempt", e)
                }
            }
          }

          if (!(arrayNode.size <= 0)) {
            logger.warn("Data that failed to load : " + arrayNode.toString)
            throw new IOException(s"Failed to load data on BE: ${starRocksStreamLoader.getLoadUrlStr} node and exceeded the max retry times.")
          }
        }
      }
    })
  }

  override def toString: String = "StarRocksStreamLoadSink"
}
