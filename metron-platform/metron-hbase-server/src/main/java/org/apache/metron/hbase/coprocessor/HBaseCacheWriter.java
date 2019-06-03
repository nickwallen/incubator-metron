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

package org.apache.metron.hbase.coprocessor;

import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.RemovalCause;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.hbase.client.HBaseClientCreator;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.apache.metron.hbase.client.HBaseSyncClientCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;

/**
 * Caffeine cache writer implementation that will write to an HBase table.
 */
public class HBaseCacheWriter implements CacheWriter<String, String> {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private HBaseConnectionFactory connectionFactory;
  private Configuration conf;
  private HBaseClientCreator clientCreator;
  private final String tableName;
  private final String columnFamily;
  private final String columnQualifier;

  /**
   * @param conf The HBase configuration.
   * @param tableName The name of the HBase table.
   * @param columnFamily The column family within the table.
   * @param columnQualifier The column qualifier within the family.
   */
  public HBaseCacheWriter(Configuration conf,
                          String tableName,
                          String columnFamily,
                          String columnQualifier) {
    this(new HBaseSyncClientCreator(), new HBaseConnectionFactory(), conf, tableName, columnFamily, columnQualifier);
  }

  /**
   * Constructor useful for testing.
   *
   * @param clientCreator Creates the {@link HBaseClient}.
   * @param connectionFactory Creates the {@link org.apache.hadoop.hbase.client.Connection} to HBase.
   * @param conf The HBase configuration.
   * @param tableName The name of the HBase table.
   * @param columnFamily The column family within the table.
   * @param columnQualifier The column qualifier within the family.
   */
  public HBaseCacheWriter(HBaseClientCreator clientCreator,
                          HBaseConnectionFactory connectionFactory,
                          Configuration conf,
                          String tableName,
                          String columnFamily,
                          String columnQualifier) {
    this.connectionFactory = connectionFactory;
    this.conf = conf;
    this.clientCreator = clientCreator;
    this.tableName = tableName;
    this.columnFamily = columnFamily;
    this.columnQualifier = columnQualifier;
  }

  /**
   * Writes a rowkey as provided by 'key' to the configured HBase table.
   * @param key value to use as a row key.
   * @param value not used.
   */
  @Override
  public void write(@Nonnull String key, @Nonnull String value) {
    LOG.debug("Calling HBase cache writer with key='{}', value='{}'", key, value);
    try (HBaseClient hbaseClient = clientCreator.create(connectionFactory, conf, tableName)) {
      LOG.debug("rowKey={}, columnFamily={}, columnQualifier={}, value={}", key, columnFamily, columnQualifier, value);

      ColumnList columns = new ColumnList().addColumn(columnFamily, columnQualifier, value);
      hbaseClient.addMutation(Bytes.toBytes(key), columns);
      hbaseClient.mutate();

    } catch (IOException e) {
      throw new RuntimeException("Error writing to HBase table", e);
    }
    LOG.debug("Done calling HBase cache writer");
  }

  @Override
  public void delete(@Nonnull String key, @Nullable String value, @Nonnull RemovalCause cause) {
    // not implemented
  }

}
