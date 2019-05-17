/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package org.apache.metron.hbase.client.integration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.apache.metron.hbase.mock.MockHBaseConnectionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the SynchronousHBaseClient
 */
public class HBaseClientIntegrationTest {

  private static final String tableName = "widgets";
  private static final String columnFamily = "W";
  private static final String columnQualifier = "column";
  private static HBaseTestingUtility util;
  private static HBaseClient client;
  private static Table table;
  private static Admin admin;
  private static final byte[] rowKey1 = Bytes.toBytes("row-key-1");
  private static final byte[] rowKey2 = Bytes.toBytes("row-key-2");

  @BeforeClass
  public static void startHBase() throws Exception {
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.master.hostname", "localhost");
    config.set("hbase.regionserver.hostname", "localhost");

    util = new HBaseTestingUtility(config);
    util.startMiniCluster();
    admin = util.getHBaseAdmin();

    // create the table
    table = util.createTable(TableName.valueOf(tableName), columnFamily);
    util.waitTableEnabled(table.getName());

    // setup the client
    MockHBaseConnectionFactory factory = new MockHBaseConnectionFactory()
            .withTable(tableName, table);
    client = HBaseClient.createSyncClient(factory, config, tableName);
  }

  @AfterClass
  public static void stopHBase() throws Exception {
    util.deleteTable(table.getName());
    util.shutdownMiniCluster();
    util.cleanupTestDir();
  }

  @After
  public void clearTable() throws Exception {

    // clear all record
    List<Delete> deletions = new ArrayList<>();
    for(Result r : table.getScanner(new Scan())) {
      deletions.add(new Delete(r.getRow()));
    }
    table.delete(deletions);
  }

  @Test
  public void testMutate() throws Exception {
    // write some values
    ColumnList columns = new ColumnList()
            .addColumn(columnFamily, columnQualifier, "value1");
    client.addMutation(rowKey1, columns, Durability.SYNC_WAL);
    client.mutate();

    // read back the value
    HBaseProjectionCriteria criteria = new HBaseProjectionCriteria()
            .addColumnFamily(columnFamily);
    client.addGet(rowKey1, criteria);
    Result[] results = client.getAll();

    // validate
    assertEquals(1, results.length);
    assertEquals("value1", getValue(results[0], columnFamily, columnQualifier));
  }

  @Test
  public void testNoMutations() throws Exception {
    // do not add any mutations before attempting to write
    int count = client.mutate();
    Assert.assertEquals(0, count);

    // attempt to read
    HBaseProjectionCriteria criteria = new HBaseProjectionCriteria()
            .addColumnFamily(columnFamily);
    client.addGet(rowKey1, criteria);
    client.addGet(rowKey2, criteria);
    Result[] results = client.getAll();

    // nothing should have been read
    assertEquals(2, results.length);
    for(Result result : results) {
      Assert.assertTrue(result.isEmpty());
    }
  }

  @Test
  public void testMutateWithTimeToLive() throws Exception {
    long timeToLive = TimeUnit.DAYS.toMillis(30);

    // add two mutations to the queue
    ColumnList cols1 = new ColumnList().addColumn(columnFamily, columnQualifier, "value1");
    client.addMutation(rowKey1, cols1, Durability.SYNC_WAL, timeToLive);
    ColumnList cols2 = new ColumnList().addColumn(columnFamily, columnQualifier, "value2");
    client.addMutation(rowKey2, cols2, Durability.SYNC_WAL, timeToLive);
    client.mutate();

    // read what was written
    HBaseProjectionCriteria criteria = new HBaseProjectionCriteria()
            .addColumnFamily(columnFamily);
    client.addGet(rowKey1, criteria);
    client.addGet(rowKey2, criteria);
    Result[] results = client.getAll();

    // validate
    assertEquals(2, results.length);
    Assert.assertEquals("value1", getValue(results[0], columnFamily, columnQualifier));
    Assert.assertEquals("value2", getValue(results[1], columnFamily, columnQualifier));
  }


  @Test
  public void testExpirationWithTimeToLive() throws Exception {
    long timeToLive = TimeUnit.MILLISECONDS.toMillis(100);

    // add two mutations to the queue
    ColumnList cols1 = new ColumnList().addColumn(columnFamily, columnQualifier, "value1");
    client.addMutation(rowKey1, cols1, Durability.SYNC_WAL, timeToLive);
    ColumnList cols2 = new ColumnList().addColumn(columnFamily, columnQualifier, "value2");
    client.addMutation(rowKey2, cols2, Durability.SYNC_WAL, timeToLive);
    client.mutate();

    HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
    criteria.addColumnFamily(columnFamily);

    // wait for a second to ensure the TTL has expired
    Thread.sleep(TimeUnit.SECONDS.toMillis(2));

    // read back both tuples
    client.addGet(rowKey1, criteria);
    client.addGet(rowKey2, criteria);
    Result[] results = client.getAll();

    // nothing should have been read; the TTL should have expired all widgets
    assertEquals(2, results.length);
    for(Result result : results) {
      Assert.assertTrue(result.isEmpty());
    }
  }

  @Test(expected = RuntimeException.class)
  public void testUnableToOpenConnection() throws IOException {
    // the factory should fail to create a connection
    HBaseConnectionFactory factory = mock(HBaseConnectionFactory.class);
    when(factory.createConnection(any())).thenThrow(new IllegalArgumentException("test exception"));

    client = HBaseClient.createSyncClient(factory, HBaseConfiguration.create(), tableName);
  }

  @Test(expected = RuntimeException.class)
  public void testFailureToMutate() throws IOException, InterruptedException {
    // used to trigger a failure condition in `HbaseClient.mutate`
    Table table = mock(Table.class);
    doThrow(new IOException("exception!")).when(table).batch(any(), any());
    HBaseConnectionFactory factory = new MockHBaseConnectionFactory().withTable(tableName, table);

    ColumnList cols1 = new ColumnList().addColumn(columnFamily, columnQualifier, "value1");
    client = HBaseClient.createSyncClient(factory, HBaseConfiguration.create(), tableName);
    client.addMutation(rowKey1, cols1, Durability.SYNC_WAL);
    client.mutate();
  }

  @Test(expected = RuntimeException.class)
  public void testFailureToGetAll() throws IOException {
    // used to trigger a failure condition in `HbaseClient.getAll`
    Table table = mock(Table.class);
    when(table.get(anyListOf(Get.class))).thenThrow(new IOException("exception!"));
    HBaseConnectionFactory factory = new MockHBaseConnectionFactory()
            .withTable(tableName, table);

    HBaseProjectionCriteria criteria = new HBaseProjectionCriteria();
    criteria.addColumnFamily(columnFamily);

    client = HBaseClient.createSyncClient(factory, HBaseConfiguration.create(), tableName);
    client.addGet(rowKey1, criteria);
    client.addGet(rowKey2, criteria);
    client.getAll();
  }

  private String getValue(Result result, String columnFamily, String columnQualifier) {
    byte[] value = result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnQualifier));
    return Bytes.toString(value);
  }
}
