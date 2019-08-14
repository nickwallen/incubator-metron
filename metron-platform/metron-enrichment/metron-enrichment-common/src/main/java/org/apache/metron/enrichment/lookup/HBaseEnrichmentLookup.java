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
package org.apache.metron.enrichment.lookup;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.enrichment.converter.EnrichmentConverter;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.converter.HbaseConverter;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs a lookup for enrichement values stored in HBase.
 */
public class HBaseEnrichmentLookup implements EnrichmentLookup {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Connection connection;
  private Table table;
  private String columnFamily;
  private HbaseConverter<EnrichmentKey, EnrichmentValue> converter;

  public HBaseEnrichmentLookup(HBaseConnectionFactory connectionFactory, String tableName, String columnFamily) throws IOException {
    this.connection = connectionFactory.createConnection(HBaseConfiguration.create());
    this.table = connection.getTable(TableName.valueOf(tableName));
    this.columnFamily = columnFamily;
    this.converter = new EnrichmentConverter();
  }

  @Override
  public boolean isInitialized() {
    return table != null;
  }

  @Override
  public boolean exists(EnrichmentKey key) throws IOException {
    return table.exists(converter.toGet(columnFamily, key));
  }

  @Override
  public LookupKV<EnrichmentKey, EnrichmentValue> get(EnrichmentKey key) throws IOException {
    Get get = converter.toGet(columnFamily, key);
    Result result = table.get(get);
    return converter.fromResult(result, columnFamily);
  }

  @Override
  public Iterable<Boolean> exists(Iterable<EnrichmentKey> keys) throws IOException {
    List<Boolean> results = new ArrayList<>();
    for(boolean exists : table.existsAll(keysToGets(keys))) {
      results.add(exists);
    }
    return results;
  }

  @Override
  public Iterable<LookupKV<EnrichmentKey, EnrichmentValue>> get(Iterable<EnrichmentKey> keys) throws IOException {
    List<LookupKV<EnrichmentKey, EnrichmentValue>> results = new ArrayList<>();
    List<Get> gets = keysToGets(keys);
    for(Result result : table.get(gets)) {
      results.add(converter.fromResult(result, columnFamily));
    }

    return results;
  }

  @Override
  public void close() {
    try {
      if(table != null) {
        table.close();
      }
    } catch(IOException e) {
      LOG.error("Error while closing HBase table", e);
    }
    try {
      if(connection != null) {
        connection.close();
      }
    } catch(IOException e) {
      LOG.error("Error while closing HBase connection",e);
    }
  }

  private List<Get> keysToGets(Iterable<EnrichmentKey> keys) {
    List<Get> ret = new ArrayList<>();
    for(EnrichmentKey key : keys) {
      Get get = converter.toGet(columnFamily, key);
      ret.add(get);
    }
    return ret;
  }
}
