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

import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.IndexingConfigurations;
import org.apache.metron.common.configuration.writer.IndexingWriterConfiguration;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.writer.BulkMessageWriter;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.hbase.integration.component.HBaseComponent;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.metron.common.configuration.writer.ConfigurationsStrategies.INDEXING;

public class JdbcWriterIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static HBaseComponent hbase;
  private static JdbcTemplate jdbcTemplate;
  private static String sensorType;

  @BeforeClass
  public static void setup() throws Exception {
    sensorType = "test";

    hbase = new HBaseComponent();
    hbase.start();

    // create a jdbc template for creating tables
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName("org.apache.phoenix.jdbc.PhoenixDriver");
    dataSource.setUrl(String.format("jdbc:phoenix:localhost:%d", hbase.getUtility().getZkCluster().getClientPort()));
    dataSource.setDefaultAutoCommit(true);

    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(hbase != null) {
      hbase.stop();
    }
  }

  /**
   * {
   *    "jdbc": {
   *       "index": "test_table",
   *       "batchSize" : 100,
   *       "batchTimeout" : 0,
   *       "enabled" : true,
   *
   *       "jdbc.driver": "org.apache.phoenix.jdbc.PhoenixDriver",
   *       "jdbc.url": "jdbc:phoenix:localhost:%d",
   *       "jdbc.username": "",
   *       "jdbc.password": ""
   *     }
   * }
   */
  @Multiline
  private static String testWrite;

  @Test
  public void testWrite() throws Exception {
    int count = 2;
    List<Tuple> tuples = createTuples(count);
    List<JSONObject> messages = createMessages(count, sensorType);

    // create the writer and its configuration; have to get the port number from the mini cluster
    JdbcWriter writer = new JdbcWriter();
    String testWriteWithPort = String.format(testWrite, hbase.getUtility().getZkCluster().getClientPort());
    WriterConfiguration writerConfig = createConfig(writer, sensorType, testWriteWithPort);

    // create the table to write to
    String tableName = writerConfig.getIndex(sensorType);
    createTable(tableName);

    // write the messages
    writer.init(null, null, writerConfig);
    BulkWriterResponse response = writer.write(sensorType, writerConfig, tuples, messages);

    // the writer response should contain only successes
    Assert.assertEquals(false, response.hasErrors());
    Assert.assertEquals(count, response.getSuccesses().size());

    for(JSONObject message: messages) {
      // ensure the message was written
      String guid = String.class.cast(message.get(Constants.GUID));
      Assert.assertEquals(message.get("field1"), getFieldValue(guid, "field1", tableName));
      Assert.assertEquals(message.get("field2"), getFieldValue(guid, "field2", tableName));
    }
  }

  private void createTable(String tableName) {
    String sql = String.format("create table %s (guid varchar primary key, field1 varchar, field2 varchar)", tableName);
    jdbcTemplate.execute(sql);
  }

  private WriterConfiguration createConfig(BulkMessageWriter writer, String sensorType, String jsonConfig) throws IOException {
    IndexingConfigurations configs = new IndexingConfigurations();
    configs.updateSensorIndexingConfig(sensorType, jsonConfig.getBytes());
    return INDEXING.createWriterConfig(writer, configs);
  }

  private String getFieldValue(String guid, String fieldName, String tableName) {
    String sql = String.format("select %s from %s where guid = '%s'", fieldName, tableName, guid);
    return jdbcTemplate.queryForObject(sql, String.class);
  }

  List<JSONObject> createMessages(int count, String sensorType) {
    List<JSONObject> messages = new ArrayList<>();
    for(int i=0; i<count; i++) {
      String guid = UUID.randomUUID().toString();

      JSONObject message = new JSONObject();
      message.put(Constants.GUID, guid);
//      message.put(Constants.SENSOR_TYPE, sensorType);
      message.put("field1", "value1-" + guid);
      message.put("field2", "value2-" + guid);
      messages.add(message);
    }

    return messages;
  }

  List<Tuple> createTuples(int count) {
    List<Tuple> tuples = new ArrayList<>();
    for(int i=0; i<count; i++) {
      Tuple tuple = Mockito.mock(Tuple.class);
      tuples.add(tuple);
    }

    return tuples;
  }
}
