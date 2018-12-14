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
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.IndexingConfigurations;
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
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.metron.common.configuration.writer.ConfigurationsStrategies.INDEXING;

public class JdbcWriterIntegrationTest {

  private static HBaseComponent hbase;
  private static JdbcTemplate template;
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

    template = new JdbcTemplate(dataSource);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(hbase != null) {
      hbase.stop();
    }
  }

  /**
   * create table messages (
   *    guid varchar primary key,
   *    created_at timestamp,
   *    src_addr varchar,
   *    src_port integer
   * )
   */
  @Multiline
  private static String createTable;

  /**
   * {
   *    "jdbc": {
   *       "batchSize" : 100,
   *       "batchTimeout" : 0,
   *       "enabled" : true,
   *
   *       "jdbc.driver": "org.apache.phoenix.jdbc.PhoenixDriver",
   *       "jdbc.url": "jdbc:phoenix:localhost:%d",
   *       "jdbc.username": "",
   *       "jdbc.password": "",
   *
   *       "jdbc.sql": "upsert into messages (guid, created_at, src_addr, src_port) values (:guid, :timestamp, :ip_src_addr, :ip_src_port)"
   *     }
   * }
   */
  @Multiline
  private static String testWrite;

  @Test
  public void testWrite() throws Exception {
    int count = 10;
    List<Tuple> tuples = createTuples(count);
    List<JSONObject> messages = createMessages(count, sensorType);

    // create the writer and its configuration; have to get the port number from the mini cluster
    JdbcWriter writer = new JdbcWriter();
    String testWriteWithPort = String.format(testWrite, hbase.getUtility().getZkCluster().getClientPort());
    WriterConfiguration writerConfig = createConfig(writer, sensorType, testWriteWithPort);

    // create the table to write to. this needs to match the upsert statement
    template.execute(createTable);

    // write the messages
    writer.init(null, null, writerConfig);
    BulkWriterResponse response = writer.write(sensorType, writerConfig, tuples, messages);

    // the writer response should contain only successes
    Assert.assertEquals(false, response.hasErrors());
    Assert.assertEquals(count, response.getSuccesses().size());

    // ensure each of the messages were written to the table
    for(JSONObject message: messages) {
      String guid = String.class.cast(message.get(Constants.GUID));

      // the message has a Long called 'timestamp' stored as a Timestamp called 'created_at' in the table
      Assert.assertEquals(
              new Timestamp(Long.class.cast(message.get("timestamp"))),
              query("select created_at from messages where guid = ?", guid, Timestamp.class));

      // the message has a String called 'ip_src_addr' stored as a varchar called 'src_addr' in the table
      Assert.assertEquals(
              message.get("ip_src_addr"),
              query("select src_addr from messages where guid = ?", guid, String.class));

      // the message has an int called 'ip_src_port' stored as an Integer called 'src_port' in the table
      Assert.assertEquals(
              message.get("ip_src_port"),
              query("select src_port from messages where guid = ?", guid, Integer.class));
    }
  }

  private <T> T query(String sql, String guid, Class<T> clazz) {
    return template.queryForObject(sql, new Object[] { guid }, clazz);
  }

  private WriterConfiguration createConfig(BulkMessageWriter writer, String sensorType, String jsonConfig) throws IOException {
    IndexingConfigurations configs = new IndexingConfigurations();
    configs.updateSensorIndexingConfig(sensorType, jsonConfig.getBytes());
    return INDEXING.createWriterConfig(writer, configs);
  }

  List<JSONObject> createMessages(int count, String sensorType) {
    List<JSONObject> messages = new ArrayList<>();
    for(int i=0; i<count; i++) {
      String guid = UUID.randomUUID().toString();

      JSONObject message = new JSONObject();
      message.put(Constants.GUID, guid);
      message.put(Constants.SENSOR_TYPE, sensorType);
      message.put(Constants.Fields.SRC_ADDR.getName(), "192.168.1.1");
      message.put(Constants.Fields.SRC_PORT.getName(), 22);
      message.put(Constants.Fields.TIMESTAMP.getName(), System.currentTimeMillis());
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
