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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.collections4.CollectionUtils.size;

/**
 * Reads records from HBase using a {@link Table}.
 */
public class TableHBaseReader implements HBaseReader {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The batch of queued get operations.
   */
  private List<Get> gets;

  /**
   * The HBase table this client interacts with.
   */
  private Table table;

  /**
   * @param connection The HBase connection. The lifecycle of this connection should be managed externally.
   * @param tableName The name of the HBase table to read.
   */
  public TableHBaseReader(Connection connection, String tableName) {
    gets = new ArrayList<>();
    try {
      table = connection.getTable(TableName.valueOf(tableName));
    } catch (Exception e) {
      String msg = String.format("Unable to connect to HBase table '%s'", tableName);
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);
    }
  }

  @Override
  public void addGet(byte[] rowKey, HBaseProjectionCriteria criteria) {
    Get get = new Get(rowKey);

    // define which column families and columns are needed
    if (criteria != null) {
      criteria.getColumnFamilies().forEach(cf -> get.addFamily(cf));
      criteria.getColumns().forEach(col -> get.addColumn(col.getColumnFamily(), col.getQualifier()));
    }

    // queue the get
    this.gets.add(get);
  }

  @Override
  public Result[] getAll() {
    try {
      return table.get(gets);

    } catch (Exception e) {
      String msg = String.format("'%d' HBase read(s) failed on table '%s'", size(gets), tableName(table));
      LOG.error(msg, e);
      throw new RuntimeException(msg, e);

    } finally {
      gets.clear();
    }
  }

  @Override
  public void clearGets() {
    gets.clear();
  }

  @Override
  public List<String> scanRowKeys() throws IOException {
    Scan scan = new Scan();
    ResultScanner scanner = table.getScanner(scan);
    List<String> rows = new ArrayList<>();
    for (Result r = scanner.next(); r != null; r = scanner.next()) {
      rows.add(Bytes.toString(r.getRow()));
    }
    return rows;
  }

  @Override
  public void close() throws IOException {
    if(table != null) {
      table.close();
    }
  }

  /**
   * Returns the name of the HBase table.
   * <p>Attempts to avoid any null pointers that might be encountered along the way.
   * @param table The table to retrieve the name of.
   * @return The name of the table
   */
  private static String tableName(Table table) {
    String tableName = "null";
    if(table != null) {
      if(table.getName() != null) {
        tableName = table.getName().getNameAsString();
      }
    }
    return tableName;
  }
}
