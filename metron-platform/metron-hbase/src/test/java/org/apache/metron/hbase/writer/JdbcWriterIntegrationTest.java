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

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.metron.common.configuration.IndexingConfigurations;
import org.apache.metron.common.configuration.writer.IndexingWriterConfiguration;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.hbase.integration.component.HBaseComponent;
import org.apache.metron.integration.ComponentRunner;
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

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class JdbcWriterIntegrationTest {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static HBaseComponent hbase;
  private static String jdbcDriver;
  private static String jdbcUrl;
  private static JdbcTemplate jdbcTemplate;
  private static BasicDataSource dataSource;

  @BeforeClass
  public static void setup() throws Exception {
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

    // create the table to write data to
    jdbcTemplate = new JdbcTemplate(dataSource);
    jdbcTemplate.execute("create table test (guid varchar primary key, field varchar, another varchar)");
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

  @Test
  public void testWrite() throws Exception {
    JdbcWriter writer = new JdbcWriter();
    writer.driver = jdbcDriver;
    writer.url = jdbcUrl;

    // TODO fix this
    writer.init(null, null, null);

    List<Tuple> tuples = new ArrayList<>();
    Tuple tuple1 = Mockito.mock(Tuple.class);
    tuples.add(tuple1);

    Tuple tuple2 = Mockito.mock(Tuple.class);
    tuples.add(tuple2);

    List<JSONObject> messages = new ArrayList<>();
    JSONObject message1 = new JSONObject();
    String guid1 = UUID.randomUUID().toString();
    message1.put("guid", guid1);
    message1.put("field", "value1");
    message1.put("another", "another1");
    messages.add(message1);

    JSONObject message2 = new JSONObject();
    String guid2 = UUID.randomUUID().toString();
    message2.put("guid", guid2);
    message2.put("field", "value2");
    messages.add(message2);

    // TODO table name not coming from the 'index'; seems to be using the sensor name for some reason
    IndexingConfigurations broConfig = new IndexingConfigurations();
    IndexingWriterConfiguration configuration = new IndexingWriterConfiguration("test", broConfig);
    BulkWriterResponse response = writer.write("test", configuration, tuples, messages);

    // the writer response should contain only successes
    Assert.assertEquals(false, response.hasErrors());
    Assert.assertEquals(2, response.getSuccesses().size());

    // validate message 1
    Assert.assertEquals(message1.get("field"), getFieldValue(guid1));
    Assert.assertEquals(message2.get("field"), getFieldValue(guid2));
  }

  private String getFieldValue(String guid) {
    return jdbcTemplate.queryForObject("select field from test where guid = ?", new Object[] { guid }, String.class);
  }

}
