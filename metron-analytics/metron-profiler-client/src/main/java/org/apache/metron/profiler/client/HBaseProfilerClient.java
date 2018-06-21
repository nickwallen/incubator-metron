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

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.ArrayUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.common.utils.SerDeUtils;
import org.apache.metron.profiler.ProfilePeriod;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * The default implementation of a ProfilerClient that fetches profile data persisted in HBase.
 */
public class HBaseProfilerClient implements ProfilerClient {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Used to access the profile data stored in HBase.
   */
  private HTableInterface table;

  /**
   * Generates the row keys necessary to scan HBase.
   */
  private RowKeyBuilder rowKeyBuilder;

  /**
   * Knows how profiles are organized in HBase.
   */
  private ColumnBuilder columnBuilder;

  public HBaseProfilerClient(HTableInterface table, RowKeyBuilder rowKeyBuilder, ColumnBuilder columnBuilder) {
    setTable(table);
    setRowKeyBuilder(rowKeyBuilder);
    setColumnBuilder(columnBuilder);
  }

  /**
   * Fetches all of the data values associated with a Profile.
   *
   * @param clazz       The type of values stored by the profile.
   * @param profile     The name of the profile.
   * @param entity      The name of the entity.
   * @param groups      The groups used to sort the profile data.
   * @param durationAgo How far in the past to fetch values from.
   * @param unit        The time unit of 'durationAgo'.
   * @param defaultValue The default value to specify.  If empty, the result will be sparse.
   * @param <T>         The type of values stored by the Profile.
   * @return A list of values.
   */
  @Override
  public <T> List<T> fetch(Class<T> clazz, String profile, String entity, List<Object> groups, long durationAgo, TimeUnit unit, Optional<T> defaultValue) {
    long end = System.currentTimeMillis();
    long start = end - unit.toMillis(durationAgo);
    return fetch(clazz, profile, entity, groups, start, end, defaultValue);
  }

  /**
   * Fetch the values stored in a profile based on a start and end timestamp.
   *
   * @param clazz   The type of values stored by the profile.
   * @param profile The name of the profile.
   * @param entity  The name of the entity.
   * @param groups  The groups used to sort the profile data.
   * @param start   The start time in epoch milliseconds.
   * @param end     The end time in epoch milliseconds.
   * @param defaultValue The default value to specify.  If empty, the result will be sparse.
   * @param <T>     The type of values stored by the profile.
   * @return A list of values.
   */
  @Override
  public <T> List<T> fetch(Class<T> clazz, String profile, String entity, List<Object> groups, long start, long end, Optional<T> defaultValue) {
    List<byte[]> rowKeysToFetch = rowKeyBuilder.rowKeys(profile, entity, groups, start, end);
    return fetchByRowKey(rowKeysToFetch, clazz, defaultValue);
  }

  /**
   * Fetch the values stored in a profile based on a set of timestamps.
   *
   * @param clazz      The type of values stored by the profile.
   * @param profile    The name of the profile.
   * @param entity     The name of the entity.
   * @param groups     The groups used to sort the profile data.
   * @param periods    The set of profile measurement periods
   * @param defaultValue The default value to specify.  If empty, the result will be sparse.
   * @return A list of values.
   */
  @Override
  public <T> List<T> fetch(Class<T> clazz, String profile, String entity, List<Object> groups, Iterable<ProfilePeriod> periods, Optional<T> defaultValue) {
    List<byte[]> rowKeysToFetch = rowKeyBuilder.rowKeys(profile, entity, groups, periods);
    return fetchByRowKey(rowKeysToFetch, clazz, defaultValue);
  }

  /**
   * Fetches the values stored in a profile by the row key.
   *
   * @param rowKeysToFetch The row keys to fetch.
   * @param clazz The type of values stored by the profile.
   * @param defaultValue The default value to specify.  If empty, the result will be sparse.
   * @param <T> The type of values stored by the profile.
   * @return A list of values.
   */
  private <T> List<T> fetchByRowKey(List<byte[]> rowKeysToFetch, Class<T> clazz, Optional<T> defaultValue) {
    // create a get for each row key
    List<Get> gets = new ArrayList<>();
    for(byte[] rowKey: rowKeysToFetch) {
      gets.add(createGet(rowKey, columnBuilder));
    }

    Result[] results = submit(gets);
    return extractValues(results, clazz, defaultValue);
  }

  /**
   * Create a Get request.
   *
   * @param rowKey The row key.
   * @param columnBuilder Defines which columns to get.
   * @return A get request.
   */
  private Get createGet(byte[] rowKey, ColumnBuilder columnBuilder) {
    byte[] columnFamily = Bytes.toBytes(columnBuilder.getColumnFamily());
    Get get = new Get(rowKey);

    for(String column: columnBuilder.getColumns()) {
      byte[] columnQualifier = columnBuilder.getColumnQualifier(column);
      get.addColumn(columnFamily, columnQualifier);
    }

    LOG.debug("Created HBase Get expecting {} column(s)", columnBuilder.getColumns().size());
    return get;
  }

  /**
   * Submit a set of requests to HBase.
   *
   * @param gets The requests to submit.
   * @return The results returned by HBase.
   */
  private Result[] submit(List<Get> gets) {
    LOG.debug("Submitting {} get(s) to HBase", gets.size());
    Result[] results;
    try {
      results = table.get(gets);

    } catch(IOException e) {
      throw new RuntimeException(e);
    }

    LOG.debug("Received {} result(s) from HBase", ArrayUtils.length(results));
    return results;
  }

  /**
   * Submits multiple Gets to HBase and deserialize the results.
   *
   * @param results         The HBase results.
   * @param clazz           The type expected in return.
   * @param defaultValue    The default value to specify.  If empty, the result will be sparse.
   * @param <T>             The type expected in return.
   * @return
   */
  private <T> List<T> extractValues(Result[] results, Class<T> clazz, Optional<T> defaultValue) {
    List<Map<String, Object>> values = new ArrayList<>();
    byte[] columnFamily = Bytes.toBytes(columnBuilder.getColumnFamily());

    // for each result...
    for(int i = 0;i < results.length;++i) {
      Result result = results[i];
      Map<String, Object> value = new HashMap<>();

      // for each column...
      for(String column: columnBuilder.getColumns()) {
        byte[] columnQualifier = columnBuilder.getColumnQualifier(column);

        boolean exists = result.containsColumn(columnFamily, columnQualifier);
        if(!exists && defaultValue.isPresent()) {

          // TODO this should ONLY be for the value field, I believe

          value.put(column, defaultValue.get());
          LOG.trace("Using default value; column={}", column);

        } else if(exists) {

          // TODO does the ColumnBuilder have to tell us the type of each field?

          byte[] val = result.getValue(columnFamily, columnQualifier);
          value.put(column, SerDeUtils.fromBytes(val, clazz));
          LOG.trace("Found value; column={}", column);
        }
      }

      values.add(value);
    }

    LOG.debug("Extracted {} value(s) from HBase", values.size());
    return values;
  }

  public void setTable(HTableInterface table) {
    this.table = table;
  }

  public void setRowKeyBuilder(RowKeyBuilder rowKeyBuilder) {
    this.rowKeyBuilder = rowKeyBuilder;
  }

  public void setColumnBuilder(ColumnBuilder columnBuilder) {
    this.columnBuilder = columnBuilder;
  }
}
