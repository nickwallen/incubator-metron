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
package org.apache.metron.hbase.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.List;

public class HBaseClient implements HBaseReader, HBaseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Connection connection;
  private HBaseReader reader;
  private HBaseWriter writer;

  // TODO replace createSyncClient with HBaseSyncClientCreator?

  /**
   * Creates a synchronous {@link HBaseClient}.
   *
   * @param factory The connection factory.
   * @param configuration The HBase configuration.
   * @param tableName The name of the HBase table.
   * @return An {@link HBaseClient} that behaves synchronously.
   */
  public static HBaseClient createSyncClient(HBaseConnectionFactory factory,
                                             Configuration configuration,
                                             String tableName) {
    try {
      Connection connection = factory.createConnection(configuration);
      HBaseReader reader = new TableHBaseReader(connection, tableName);
      HBaseWriter writer = new TableHBaseWriter(connection, tableName);
      return new HBaseClient(connection, reader, writer);

    } catch (Exception e) {
      String msg = String.format("Unable to open connection to HBase for table '%s'", tableName);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  /**
   * Creates an asynchronous {@link HBaseClient}.
   *
   * @param factory The connection factory.
   * @param configuration The HBase configuration.
   * @param tableName The name of the HBase table.
   * @return An {@link HBaseClient} that behaves asynchronously.
   */
  public static HBaseClient createAsyncClient(HBaseConnectionFactory factory,
                                              Configuration configuration,
                                              String tableName) {
    try {
      Connection connection = factory.createConnection(configuration);
      HBaseReader reader = new TableHBaseReader(connection, tableName);
      HBaseWriter writer = new BufferedMutatorHBaseWriter(connection, tableName);
      return new HBaseClient(connection, reader, writer);

    } catch (Exception e) {
      String msg = String.format("Unable to open connection to HBase for table '%s'", tableName);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  public HBaseClient(Connection connection, HBaseReader reader, HBaseWriter writer) {
    // the connection will be closed when the client is closed
    this.connection = connection;
    this.reader = reader;
    this.writer = writer;
  }

  @Override
  public void addGet(byte[] rowKey, HBaseProjectionCriteria criteria) {
    reader.addGet(rowKey, criteria);
  }

  @Override
  public Result[] getAll() {
    return reader.getAll();
  }

  @Override
  public void clearGets() {
    reader.clearGets();
  }

  @Override
  public List<String> scanRowKeys() throws IOException {
    return reader.scanRowKeys();
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols) {
    writer.addMutation(rowKey, cols);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability) {
    writer.addMutation(rowKey, cols, durability);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability, Long timeToLiveMillis) {
    writer.addMutation(rowKey, cols, durability, timeToLiveMillis);
  }

  @Override
  public int mutate() {
    return writer.mutate();
  }

  @Override
  public void clearMutations() {
    writer.clearMutations();
  }

  @Override
  public void close() throws IOException {
    reader.close();
    writer.close();
    if(connection != null) {
      connection.close();
    }
  }
}
