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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class JdbcWriterTest {

  private static HBaseComponent hbase;

//  @BeforeClass
//  public static void setup() throws Exception {
//      hbase = new HBaseComponent();
//      hbase.start();
//  }
//
//  @AfterClass
//  public static void tearDown() throws Exception {
//    if(hbase != null) {
//      hbase.stop();
//    }
//  }

  // TODO setup in-memory HBase
  // TODO call this an integration test?

  @Test
  public void test() throws Exception {
//    Table table = hbase.getUtility().createTable(TableName.valueOf("table"), "family");
//    Assert.assertNotNull(table);

    JdbcWriter writer = new JdbcWriter();
    writer.driver = "org.apache.phoenix.jdbc.PhoenixDriver";
    writer.url = "jdbc:phoenix:localhost:2181";

    // TODO fix this
    writer.init(null, null, null);

    List<Tuple> tuples = new ArrayList<>();
    Tuple tuple1 = Mockito.mock(Tuple.class);
    tuples.add(tuple1);

    Tuple tuple2 = Mockito.mock(Tuple.class);
    tuples.add(tuple2);

    List<JSONObject> messages = new ArrayList<>();
    JSONObject message1 = new JSONObject();
    message1.put("guid", UUID.randomUUID());
    message1.put("field", "value1");
    message1.put("another", "another1");
    messages.add(message1);

    JSONObject message2 = new JSONObject();
    message2.put("guid", UUID.randomUUID());
    message2.put("field", "value2");
    messages.add(message2);

    // TODO table name not coming from the 'index'; seems to be using the sensor name for some reason
    IndexingConfigurations broConfig = new IndexingConfigurations();
    IndexingWriterConfiguration configuration = new IndexingWriterConfiguration("test", broConfig);
    BulkWriterResponse response = writer.write("test", configuration, tuples, messages);

    Assert.assertEquals(false, response.hasErrors());
    Assert.assertEquals(2, response.getSuccesses().size());
    //Assert.assertEquals(1, hbase.getUtility().countRows(table));
    // TODO use start-hbase.sh from command-line to start-up HBase
  }


}
