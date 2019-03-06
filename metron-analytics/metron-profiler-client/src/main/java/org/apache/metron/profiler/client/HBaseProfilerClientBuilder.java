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
package org.apache.metron.profiler.client;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.metron.hbase.HTableProvider;
import org.apache.metron.hbase.TableProvider;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.apache.metron.profiler.hbase.SaltyRowKeyBuilder;
import org.apache.metron.profiler.hbase.ValueOnlyColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_COLUMN_FAMILY;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE_PROVIDER;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD_UNITS;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_SALT_DIVISOR;
import static org.apache.metron.profiler.client.stellar.Util.getPeriodDurationInMillis;

/**
 * Builds a {@link HBaseProfilerClient}.
 */
public class HBaseProfilerClientBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Map<String, Object> globals;
  private HTableInterface table;

  public HBaseProfilerClientBuilder() {
    this.globals = new HashMap<>();
  }

  public HBaseProfilerClientBuilder withGlobals(Map<String, Object> globals) {
    this.globals = globals;
    return this;
  }

  public HBaseProfilerClientBuilder withTable(HTableInterface table) {
    this.table = table;
    return this;
  }

  public HBaseProfilerClient build() {
    if(globals == null) {
      globals = new HashMap<>();
    }

    if(table == null) {
      table = getTable(globals);
    }

    return new HBaseProfilerClient(
            table,
            rowKeyBuilder(globals),
            columnBuilder(globals),
            getPeriodDurationInMillis(globals));
  }

  private ColumnBuilder columnBuilder(Map<String, Object> global) {
    String columnFamily = PROFILER_COLUMN_FAMILY.get(global, String.class);
    return new ValueOnlyColumnBuilder(columnFamily);
  }

  private RowKeyBuilder rowKeyBuilder(Map<String, Object> global) {
    // how long is the profile period?
    long duration = PROFILER_PERIOD.get(global, Long.class);
    LOG.debug("profiler client: {}={}", PROFILER_PERIOD, duration);

    // which units are used to define the profile period?
    String configuredUnits = PROFILER_PERIOD_UNITS.get(global, String.class);
    TimeUnit units = TimeUnit.valueOf(configuredUnits);
    LOG.debug("profiler client: {}={}", PROFILER_PERIOD_UNITS, units);

    // what is the salt divisor?
    Integer saltDivisor = PROFILER_SALT_DIVISOR.get(global, Integer.class);
    LOG.debug("profiler client: {}={}", PROFILER_SALT_DIVISOR, saltDivisor);

    return new SaltyRowKeyBuilder(saltDivisor, duration, units);
  }

  private HTableInterface getTable(Map<String, Object> global) {
    String tableName = PROFILER_HBASE_TABLE.get(global, String.class);
    TableProvider provider = getTableProvider(global);
    try {
      return provider.getTable(HBaseConfiguration.create(), tableName);

    } catch (IOException e) {
      throw new IllegalArgumentException(String.format("Unable to access table: %s", tableName), e);
    }
  }

  private TableProvider getTableProvider(Map<String, Object> global) {
    String clazzName = PROFILER_HBASE_TABLE_PROVIDER.get(global, String.class);
    TableProvider provider;
    try {
      @SuppressWarnings("unchecked")
      Class<? extends TableProvider> clazz = (Class<? extends TableProvider>) Class.forName(clazzName);
      provider = clazz.getConstructor().newInstance();

    } catch (Exception e) {
      provider = new HTableProvider();
    }
    return provider;
  }
}
