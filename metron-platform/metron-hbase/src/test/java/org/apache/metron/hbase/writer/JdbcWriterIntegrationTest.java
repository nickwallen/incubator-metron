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
  private static String jdbcDriver;
  private static String jdbcUrl;
  private static JdbcTemplate jdbcTemplate;
  private static BasicDataSource dataSource;
  private static String sensorType;

  @BeforeClass
  public static void setup() throws Exception {
    sensorType = "test";

    hbase = new HBaseComponent();
    hbase.start();

    // gather the connection info
    jdbcDriver = "org.apache.phoenix.jdbc.PhoenixDriver";
    jdbcUrl = String.format("jdbc:phoenix:localhost:%d", hbase.getUtility().getZkCluster().getClientPort());
    log.debug("Using driver={}, url={}", jdbcDriver, jdbcUrl);

    // create a connection
    dataSource = new BasicDataSource();
    dataSource.setDriverClassName(jdbcDriver);
    dataSource.setUrl(jdbcUrl);
    dataSource.setDefaultAutoCommit(true);
    jdbcTemplate = new JdbcTemplate(dataSource);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if(hbase != null) {
      hbase.stop();
    }
    if(dataSource != null) {
      dataSource.close();
    }
  }

  /**
   * {
   *    "jdbc": {
   *       "index": "test_table",
   *       "batchSize" : 100,
   *       "batchTimeout" : 0,
   *       "enabled" : true
   *     }
   * }
   */
  @Multiline
  private static String jdbcWriterConfig;

  @Test
  public void testWrite() throws Exception {
    List<Tuple> tuples = new ArrayList<>();
    Tuple tuple1 = Mockito.mock(Tuple.class);
    tuples.add(tuple1);

    Tuple tuple2 = Mockito.mock(Tuple.class);
    tuples.add(tuple2);

    List<JSONObject> messages = new ArrayList<>();
    JSONObject message1 = new JSONObject();
    String guid1 = UUID.randomUUID().toString();
    message1.put(Constants.GUID, guid1);
    message1.put("field1", "value1-" + guid1);
    message1.put("field2", "value2-" + guid1);
    messages.add(message1);

    JSONObject message2 = new JSONObject();
    String guid2 = UUID.randomUUID().toString();
    message2.put(Constants.GUID, guid2);
    message2.put("field1", "value1-" + guid2);
    message2.put("field2", "value2-" + guid2);
    messages.add(message2);

    // create the writer
    JdbcWriter writer = new JdbcWriter();
    writer.driver = jdbcDriver;
    writer.url = jdbcUrl;

    // create the writer configuration
    WriterConfiguration configuration = configuration(writer, sensorType);

    // create the table to write to
    String tableName = configuration.getIndex(sensorType);
    createTable(tableName);

    // write the messages
    writer.init(null, null, null);
    BulkWriterResponse response = writer.write(sensorType, configuration, tuples, messages);

    // the writer response should contain only successes
    Assert.assertEquals(false, response.hasErrors());
    Assert.assertEquals(2, response.getSuccesses().size());

    // validate message 1
    Assert.assertEquals(message1.get("field1"), getFieldValue(guid1, "field1", tableName));
    Assert.assertEquals(message1.get("field2"), getFieldValue(guid1, "field2", tableName));

    // validate message 2
    Assert.assertEquals(message2.get("field1"), getFieldValue(guid2, "field1", tableName));
    Assert.assertEquals(message2.get("field2"), getFieldValue(guid2, "field2", tableName));
  }

  private void createTable(String tableName) {
    String sql = String.format("create table %s (guid varchar primary key, field1 varchar, field2 varchar)", tableName);
    jdbcTemplate.execute(sql);
  }

  private WriterConfiguration configuration(BulkMessageWriter writer, String sensorType) throws IOException {
    IndexingConfigurations configs = new IndexingConfigurations();
    configs.updateSensorIndexingConfig(sensorType, jdbcWriterConfig.getBytes());
    return INDEXING.createWriterConfig(writer, configs);
  }

  private String getFieldValue(String guid, String fieldName, String tableName) {
    String sql = String.format("select %s from %s where guid = '%s'", fieldName, tableName, guid);
    return jdbcTemplate.queryForObject(sql, String.class);
  }

}
