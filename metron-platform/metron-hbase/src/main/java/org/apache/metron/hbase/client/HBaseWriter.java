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
import org.apache.metron.hbase.bolt.mapper.ColumnList;

import java.io.Closeable;

/**
 * An {@link HBaseWriter} is used to write data to HBase.
 */
public interface HBaseWriter extends Closeable {

  /**
   * Enqueues a {@link org.apache.hadoop.hbase.client.Mutation} such as a put or
   * increment.  The operation is enqueued for later execution.
   *
   * @param rowKey     The row key of the Mutation.
   * @param cols       The columns affected by the Mutation.
   */
  void addMutation(byte[] rowKey, ColumnList cols);

  /**
   * Enqueues a {@link org.apache.hadoop.hbase.client.Mutation} such as a put or
   * increment.  The operation is enqueued for later execution.
   *
   * @param rowKey     The row key of the Mutation.
   * @param cols       The columns affected by the Mutation.
   * @param durability The durability of the mutation.
   */
  void addMutation(byte[] rowKey, ColumnList cols, Durability durability);

  /**
   * Enqueues a {@link org.apache.hadoop.hbase.client.Mutation} such as a put or
   * increment.  The operation is enqueued for later execution.
   *
   * @param rowKey           The row key of the Mutation.
   * @param cols             The columns affected by the Mutation.
   * @param durability       The durability of the mutation.
   * @param timeToLiveMillis The time to live in milliseconds.
   */
  void addMutation(byte[] rowKey, ColumnList cols, Durability durability, Long timeToLiveMillis);

  /**
   * Ensures that all pending mutations have completed.
   *
   * @return The number of operations completed.
   */
  int mutate();

  /**
   * Clears all pending mutations.
   */
  void clearMutations();
}
