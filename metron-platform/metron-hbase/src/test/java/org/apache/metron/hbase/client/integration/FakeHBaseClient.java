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
package org.apache.metron.hbase.client.integration;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Result;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;
import org.apache.metron.hbase.client.HBaseClient;

import java.io.IOException;
import java.util.List;

public class FakeHBaseClient extends HBaseClient {

  private static Result[] results;

  public FakeHBaseClient() {
    // TODO need HBaseClient to be an interface.
    super(null, null, null);
  }

  @Override
  public void addGet(byte[] rowKey, HBaseProjectionCriteria criteria) {
    // TODO
  }

  @Override
  public Result[] getAll() {
    return results;
  }

  public void setResults(Result[] theResults) {
    results = theResults;
  }

  @Override
  public void clearGets() {
    super.clearGets();
  }

  @Override
  public List<String> scanRowKeys() throws IOException {
    return super.scanRowKeys();
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols) {
    super.addMutation(rowKey, cols);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability) {
    super.addMutation(rowKey, cols, durability);
  }

  @Override
  public void addMutation(byte[] rowKey, ColumnList cols, Durability durability, Long timeToLiveMillis) {
    super.addMutation(rowKey, cols, durability, timeToLiveMillis);
  }

  @Override
  public int mutate() {
    return super.mutate();
  }

  @Override
  public void clearMutations() {
    super.clearMutations();
  }

  @Override
  public void close() throws IOException {
    super.close();
  }
}
