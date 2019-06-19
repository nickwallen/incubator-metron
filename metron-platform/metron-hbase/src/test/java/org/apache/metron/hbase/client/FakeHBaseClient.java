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

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * An {@link HBaseClient} useful for testing.
 *
 * <p>Maintains a static, in-memory set of records to mimic the behavior of
 * an {@link HBaseClient} that interacts with HBase.
 */
public class FakeHBaseClient implements HBaseClient {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The mutations that have been persisted.  Represents mutations that would
   * have been written to HBase.
   *
   * <p>This is static so that all instantiated clients 'see' the same set of records.
   */
  private static Map<ByteBuffer, ColumnList> persisted = Collections.synchronizedMap(new HashMap<>());

  /**
   * Represents a mutation that was submitted to the {@link FakeHBaseClient}.
   */
  public static class Mutation {
    public byte[] rowKey;
    public ColumnList columnList;

    public Mutation(byte[] rowKey, ColumnList columnList) {
      this.rowKey = rowKey;
      this.columnList = columnList;
    }
  }

  /**
   * The set of queued or pending mutations.
   */
  private List<Mutation> queuedMutations;

  /**
   * The set of queued or pending gets.
   */
  private List<ByteBuffer> queuedGets;

  public FakeHBaseClient() {
    queuedGets = new ArrayList<>();
    queuedMutations = new ArrayList<>();
  }

  /**
   * Deletes all records persisted in the static, in-memory collection.
   */
  public void deleteAll() {
    persisted.clear();
  }

  /**
   * Returns all mutations that have been persisted in the static, in-memory collection.
   */
  public List<Mutation> getAllPersisted() {
    return persisted.entrySet()
            .stream()
            .map(entry -> new Mutation(entry.getKey().array(), entry.getValue()))
            .collect(Collectors.toList());
  }

  @Override
  public void addGet(byte[] rowKey, HBaseProjectionCriteria criteria) {
    queuedGets.add(ByteBuffer.wrap(rowKey));
  }

  @Override
  public Result[] getAll() {
    LOG.debug("Looking for {} record(s)", queuedGets.size());
    List<Result> results = new ArrayList<>();
    for (int i = 0; i < queuedGets.size(); i++) {
      ByteBuffer rowKey = queuedGets.get(i);
      Result result;
      if (persisted.containsKey(rowKey)) {
        ColumnList cols = persisted.get(rowKey);
        result = matchingResult(cols);

      } else {
        result = emptyResult();
      }
      results.add(result);
    }

    clearGets();
    return results.stream().toArray(Result[]::new);
  }

  private static Result matchingResult(ColumnList columns) {
    Result result = mock(Result.class);
    for (ColumnList.Column column : columns.getColumns()) {
      LOG.debug("Found matching column; {}:{}", Bytes.toString(column.getFamily()),
              Bytes.toString(column.getQualifier()));

      // Result.containsColumn(family, qualifier) should return true
      when(result.containsColumn(eq(column.getFamily()), eq(column.getQualifier())))
              .thenReturn(true);

      // Result.getValue(family, qualifier) should return the value
      when(result.getValue(eq(column.getFamily()), eq(column.getQualifier())))
              .thenReturn(column.getValue());
    }

    return result;
  }

  private static Result emptyResult() {
    Result result = mock(Result.class);
    when(result.containsColumn(any(), any()))
            .thenReturn(false);
    return result;
  }

  @Override
  public void clearGets() {
    queuedGets.clear();
  }

  @Override
  public List<String> scanRowKeys() {
    return persisted.keySet()
            .stream()
            .map(bytes -> String.valueOf(bytes))
            .collect(Collectors.toList());
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols) {
    for (ColumnList.Column column : cols.getColumns()) {
      String family = Bytes.toString(column.getFamily());
      String qualifier = Bytes.toString(column.getQualifier());
      LOG.debug("Queuing mutation column; {}:{}", family, qualifier);
    }
    queuedMutations.add(new Mutation(rowKey, cols));
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability) {
    // ignore durability
    addMutation(rowKey, cols);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability, Long timeToLiveMillis) {
    // ignore durability and time-to-live
    addMutation(rowKey, cols);
  }

  @Override
  public int mutate() {
    int numberOfMutations = queuedMutations.size();

    // persist each queued mutation
    queuedMutations.forEach(mutation -> persisted.put(ByteBuffer.wrap(mutation.rowKey), mutation.columnList));
    clearMutations();

    LOG.debug("Wrote {} record(s); now have {} record(s) in all", numberOfMutations, persisted.size());
    return numberOfMutations;
  }

  @Override
  public void clearMutations() {
    queuedMutations.clear();
  }

  @Override
  public void close() {
    // nothing to do
  }
}
