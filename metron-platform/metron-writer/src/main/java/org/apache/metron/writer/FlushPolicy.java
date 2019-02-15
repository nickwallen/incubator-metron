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
package org.apache.metron.writer;

import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.writer.BulkWriterResponse;

/**
 * A policy used by the {@link org.apache.metron.writer.BulkWriterComponent} to
 * determine when to flush and how flushes are handled.
 */
public interface FlushPolicy {

  /**
   * Allows the policy to define when a flush should occur.
   *
   * <p>This method is called each time a message is passed to the {@link BulkWriterComponent}.
   *
   * @param sensorType
   * @param configurations
   * @param batchSize
   * @return
   */
  boolean shouldFlush(String sensorType, WriterConfiguration configurations, int batchSize);

  /**
   * Allows a policy to define what should happen when a flush occurs.
   *
   * <p>This method is called immediately after a batch has been flushed.
   *
   * @param sensorType The type of sensor generating the messages
   * @param response A response containing successes and failures within the batch
   */
  void onFlush(String sensorType, BulkWriterResponse response);
}
