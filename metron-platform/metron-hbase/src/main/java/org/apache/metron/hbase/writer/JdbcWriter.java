/**
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
package org.apache.metron.hbase.writer;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.writer.BulkMessageWriter;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.metron.hbase.writer.JdbcWriterOptions.JDBC_DRIVER;
import static org.apache.metron.hbase.writer.JdbcWriterOptions.JDBC_PASSWORD;
import static org.apache.metron.hbase.writer.JdbcWriterOptions.JDBC_URL;
import static org.apache.metron.hbase.writer.JdbcWriterOptions.JDBC_USERNAME;

/**
 * Writes messages to a data source using JDBC.
 */
public class JdbcWriter implements BulkMessageWriter<JSONObject>, Serializable {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Cache<String, JdbcTemplate> templateCache;

  @Override
  public void init(Map stormConf, TopologyContext topologyContext, WriterConfiguration config) {
    // TODO make cache settings configurable by user
    Caffeine cacheBuilder = Caffeine
            .newBuilder()
            .maximumSize(100)
            .expireAfterAccess(30, TimeUnit.MINUTES);
    if (log.isDebugEnabled()) {
      cacheBuilder.recordStats();
    }
    templateCache = cacheBuilder.build();
  }

  @Override
  public BulkWriterResponse write(String sensorType,
                                  WriterConfiguration writerConfig,
                                  Iterable<Tuple> tuples,
                                  List<JSONObject> messages) {
    BulkWriterResponse response = new BulkWriterResponse();
    if (messages.size() <= 0) {
      return response;
    }

    // TODO all fields are set as strings right now; can SimpleJdbc* help choose based on the value in the table??

    // TODO simply by defining the table columns and types, the user is defining which fields from the message are persisted??

    // TODO the only thing phoenix-ish about this is the weird upsert statement

    // TODO allow user to whitelist fields?

    // the columns are determined by the first 'prototype' message
    JSONObject prototype = messages.get(0);
    List<String> fields = new ArrayList<>(prototype.keySet());
    String columns = StringUtils.join(fields, ",");
    String values = StringUtils.repeat("?", ",", fields.size());
    String table = writerConfig.getIndex(sensorType);
    String updateSql = String.format("upsert into %s (%s) values (%s)", table, columns, values);

    BatchPreparedStatementSetter setter = new BatchPreparedStatementSetter() {
      @Override
      public void setValues(PreparedStatement ps, int messageIndex) throws SQLException {
        JSONObject message = messages.get(messageIndex);
        for(int i=0; i<fields.size(); i++) {
          String field = fields.get(i);
          Object value = message.getOrDefault(field, "");
          ps.setString(i+1, value.toString());
        }
      }

      @Override
      public int getBatchSize() {
        return messages.size();
      }
    };

    try {
      getTemplate(sensorType, writerConfig).batchUpdate(updateSql, setter);
      response.addAllSuccesses(tuples);

    } catch(Exception e) {
      log.error(String.format("Failed to write %d message(s)", messages.size()), e);
      response.addAllErrors(e, tuples);
    }

    return response;
  }

  @Override
  public String getName() {
    return "jdbc";
  }

  @Override
  public void close() {
    // TODO what do we need to close?
  }

  private JdbcTemplate getTemplate(String sensorType, WriterConfiguration config) {
    // creates the template, if none already cached
    Function<String, JdbcTemplate> creator = (key) -> {
      Map<String, Object> allOptions = config.getSensorConfig(sensorType);
      log.debug("Creating JDBC connection; sensorType={}, options={}", sensorType, allOptions);

      // TODO probably not the right data source.  how does user define pooled connections?
      BasicDataSource dataSource = new BasicDataSource();
      dataSource.setDriverClassName(JDBC_DRIVER.getOrDefault(allOptions, String.class));
      dataSource.setUrl(JDBC_URL.getOrDefault(allOptions, String.class));
      dataSource.setUsername(JDBC_USERNAME.getOrDefault(allOptions, String.class));
      dataSource.setPassword(JDBC_PASSWORD.getOrDefault(allOptions, String.class));
      dataSource.setDefaultAutoCommit(true);
      return new JdbcTemplate(dataSource);
    };

    return templateCache.get(sensorType, creator);
  }
}
