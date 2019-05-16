/*
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
package org.apache.metron.hbase.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.hbase.client.HBaseConnectionFactory;

import java.io.IOException;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A mock {@link HBaseConnectionFactory} useful for testing.
 */
public class MockHBaseConnectionFactory extends HBaseConnectionFactory {

  private Table table;
  private BufferedMutator mutator;

  /**
   * @param table All connections created by this factory will return this {@link Table}.
   */
  public MockHBaseConnectionFactory(Table table) {
    this.table = table;
  }

  /**
   * @param mutator All connections created by this factory will return this {@link BufferedMutator}.
   */
  public MockHBaseConnectionFactory(BufferedMutator mutator) {
    this.mutator = mutator;
  }

  public MockHBaseConnectionFactory() {
    this.table = mock(Table.class);
    this.mutator = mock(BufferedMutator.class);
  }

  @Override
  public Connection createConnection(Configuration configuration) throws IOException {
    // the mock connection should return the table and mutator if requested
    Connection connection = mock(Connection.class);
    when(connection.getTable(any())).thenReturn(table);
    when(connection.getBufferedMutator(any(TableName.class))).thenReturn(mutator);
    when(connection.getBufferedMutator(any(BufferedMutatorParams.class))).thenReturn(mutator);
    return connection;
  }

  public Table getTable() {
    return table;
  }

  public BufferedMutator getMutator() {
    return mutator;
  }
}
