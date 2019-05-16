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

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * Writes records to HBase using a {@link BufferedMutator}.
 */
public class BufferedMutatorHBaseWriter implements HBaseWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The {@link BufferedMutator} used to write to HBase.
   */
  private BufferedMutator mutator;

  /**
   * Tracks the number of pending mutation operations.  We do not really know how many
   * are truly pending until we flush the mutator to ensure that all operations have
   * completed.
   */
  private int pending;

  /**
   * @param connection The HBase connection. The lifecycle of this connection should be managed externally.
   * @param tableName The name of the HBase table to read.
   */
  public BufferedMutatorHBaseWriter(Connection connection, String tableName) {
    pending = 0;
    try {
      mutator = connection.getBufferedMutator(TableName.valueOf(tableName));
    } catch (Exception e) {
      String msg = String.format("Unable to connect to HBase table '%s'", tableName);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols) {
    HBaseWriterParams params = new HBaseWriterParams();
    addMutation(rowKey, cols, params);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability) {
    HBaseWriterParams params = new HBaseWriterParams()
            .withDurability(durability);
    addMutation(rowKey, cols, params);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability, Long timeToLiveMillis) {
    HBaseWriterParams params = new HBaseWriterParams()
            .withDurability(durability)
            .withTimeToLive(timeToLiveMillis);
    addMutation(rowKey, cols, params);
  }

  private void addMutation(byte[] rowKey, ColumnList cols, HBaseWriterParams params) {
    try {
      if (cols.hasColumns()) {
        Put put = createPut(rowKey, params);
        addColumns(cols, put);
        mutator.mutate(put);
        pending++;
      }

      if (cols.hasCounters()) {
        Increment inc = createIncrement(rowKey, params);
        addColumns(cols, inc);
        mutator.mutate(inc);
        pending++;
      }

    } catch (Exception e) {
      String msg = String.format("Unable to write to HBase for table '%s'", mutator.getName().getNameAsString());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  @Override
  public void clearMutations() {
    // not possible with the BufferedMutator
  }

  @Override
  public int mutate() {
    try {
      // ensure that all mutations have completed
      mutator.flush();

      // reset the counter
      int count = pending;
      pending = 0;
      return count;

    } catch (Exception e) {
      String msg = String.format("Unable to write to HBase for table '%s'", mutator.getName().getNameAsString());
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  @Override
  public void close() throws IOException {
    if(mutator != null) {
      this.mutator.close();
    }
  }

  private Put createPut(byte[] rowKey, HBaseWriterParams params) {
    Put put = new Put(rowKey);
    if(params.getTimeToLiveMillis() > 0) {
      put.setTTL(params.getTimeToLiveMillis());
    }
    put.setDurability(params.getDurability());
    return put;
  }

  private void addColumns(ColumnList cols, Put put) {
    for (ColumnList.Column col: cols.getColumns()) {
      if (col.getTs() > 0) {
        put.addColumn(col.getFamily(), col.getQualifier(), col.getTs(), col.getValue());
      } else {
        put.addColumn(col.getFamily(), col.getQualifier(), col.getValue());
      }
    }
  }

  private void addColumns(ColumnList cols, Increment inc) {
    cols.getCounters().forEach(cnt ->
            inc.addColumn(cnt.getFamily(), cnt.getQualifier(), cnt.getIncrement()));
  }

  private Increment createIncrement(byte[] rowKey, HBaseWriterParams params) {
    Increment inc = new Increment(rowKey);
    if(params.getTimeToLiveMillis() > 0) {
      inc.setTTL(params.getTimeToLiveMillis());
    }
    inc.setDurability(params.getDurability());
    return inc;
  }
}
