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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.lookup.accesstracker.AccessTracker;
import org.apache.metron.enrichment.lookup.handler.HBaseContext;
import org.apache.metron.enrichment.lookup.handler.HBaseEnrichmentHandler;
import org.apache.metron.enrichment.lookup.handler.Handler;
import org.apache.metron.hbase.client.HBaseConnectionFactory;

import java.io.IOException;


public class EnrichmentLookup extends Lookup<HBaseContext, EnrichmentKey, LookupKV<EnrichmentKey, EnrichmentValue>> implements AutoCloseable {

  private Table table;
  private Connection connection;

  public EnrichmentLookup() {
    // TODO this is a bad idea? see MockEnrichmentLookup
  }

  public EnrichmentLookup(HBaseConnectionFactory connectionFactory,
                          String tableName,
                          String columnFamily,
                          AccessTracker tracker) throws IOException {
    this(connectionFactory, HBaseConfiguration.create(), tableName, columnFamily, tracker);
  }

  public EnrichmentLookup(HBaseConnectionFactory connectionFactory,
                          Configuration conf,
                          String tableName,
                          String columnFamily,
                          AccessTracker tracker) throws IOException {
    this(connectionFactory, conf, new HBaseEnrichmentHandler(columnFamily), tableName, tracker);
  }

  public EnrichmentLookup(HBaseConnectionFactory connectionFactory,
                          Configuration conf,
                          Handler<HBaseContext, EnrichmentKey, LookupKV<EnrichmentKey,EnrichmentValue>> handler,
                          String tableName,
                          AccessTracker tracker) throws IOException {
    this.connection = connectionFactory.createConnection(conf);
    this.table = connection.getTable(TableName.valueOf(tableName));
    this.setLookupHandler(handler);
    this.setAccessTracker(tracker);
  }

  public Table getTable() {
    return table;
  }

  @Override
  public void close() throws Exception {
    super.close();
    if(table != null) {
      table.close();
    }
    if(connection != null) {
      connection.close();
    }
  }
}
