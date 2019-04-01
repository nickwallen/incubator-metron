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

import org.apache.commons.collections.CollectionUtils;
import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.system.Clock;
import org.apache.metron.common.writer.BulkMessageWriter;
import org.apache.metron.common.writer.BulkMessage;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.common.writer.MessageId;
import org.apache.metron.writer.mbean.WriterMetricsPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This component manages an internal cache of messages to be written in batch.  A separate cache is used for each sensor.
 * Each time a message is written to this component, the {@link org.apache.metron.writer.FlushPolicy#shouldFlush(String, WriterConfiguration, List)}
 * method is called for each flush policy to determine if a batch of messages should be flushed.  When a flush does happen,
 * the {@link org.apache.metron.writer.FlushPolicy#postFlush(String, BulkWriterResponse)} method is called for each flush policy
 * so that any post-processing (message acknowledgement for example) can be done.  This component also ensures all messages
 * in a batch are included in the response as either a success or failure.
 *
 * @param <MESSAGE_T>
 */
public class BulkWriterComponent<MESSAGE_T> {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Map<String, List<BulkMessage<MESSAGE_T>>> sensorMessageCache = new HashMap<>();
  private List<FlushPolicy<MESSAGE_T>> flushPolicies;

  public BulkWriterComponent(int maxBatchTimeout) {
    this(maxBatchTimeout, new Clock());
  }

  public BulkWriterComponent(int maxBatchTimeout, Clock clock) {
    flushPolicies = new ArrayList<>();
    flushPolicies.add(new BatchSizePolicy<>());
    flushPolicies.add(new BatchTimeoutPolicy<>(maxBatchTimeout, clock));
    flushPolicies.add(new WriterMetricsPolicy<>());
  }

  protected BulkWriterComponent(List<FlushPolicy<MESSAGE_T>> flushPolicies) {
    this.flushPolicies = flushPolicies;
  }

  /**
   * Accepts a message to be written and stores it in an internal cache of messages.  Iterates through {@link org.apache.metron.writer.FlushPolicy}
   * implementations to determine if a batch should be flushed.
   * @param sensorType sensor type
   * @param bulkMessage message to be written
   * @param bulkMessageWriter writer that will do the actual writing
   * @param configurations writer configurations
   */
  public void write(String sensorType,
                    BulkMessage<MESSAGE_T> bulkMessage,
                    BulkMessageWriter<MESSAGE_T> bulkMessageWriter,
                    WriterConfiguration configurations) {
    List<BulkMessage<MESSAGE_T>> messages = sensorMessageCache.getOrDefault(sensorType, new ArrayList<>());
    sensorMessageCache.put(sensorType, messages);

    if(configurations.isEnabled(sensorType)) {
      // sensor enabled
      messages.add(bulkMessage);
      if(shouldFlush(sensorType, bulkMessageWriter, configurations, messages)) {
        doFlush(sensorType, bulkMessageWriter, configurations, messages);
      }

    } else if(messages.size() > 0) {
      // sensor disabled and there are messages in the cache
      // flush the cache and discard the new message
      doFlush(sensorType, bulkMessageWriter, configurations, messages);
    }
  }

  private void doFlush(String sensorType,
                       BulkMessageWriter<MESSAGE_T> bulkMessageWriter,
                       WriterConfiguration configurations,
                       List<BulkMessage<MESSAGE_T>> messages) {
      preFlush(sensorType, configurations, messages);
      BulkWriterResponse response = flush(sensorType, bulkMessageWriter, configurations, messages);
      postFlush(sensorType, response);
  }

  /**
   * Flushes a batch for a sensor type by writing messages with the supplied {@link org.apache.metron.common.writer.BulkMessageWriter}.
   * Ensures all message ids in a batch are included in the response. After messages are written the cache is cleared and
   * flush policies are reset for that sensor type.
   * @param sensorType sensor type
   * @param bulkMessageWriter writer that will do the actual writing
   * @param configurations writer configurations
   * @param messages messages to be written
   * @return The bulk writer response.
   */
  private BulkWriterResponse flush(String sensorType,
                                   BulkMessageWriter<MESSAGE_T> bulkMessageWriter,
                                   WriterConfiguration configurations,
                                   List<BulkMessage<MESSAGE_T>> messages) {
    long startTime = System.currentTimeMillis(); //no need to mock, so use real time
    BulkWriterResponse response = new BulkWriterResponse();
    Collection<MessageId> ids = messages.stream().map(BulkMessage::getId).collect(Collectors.toList());
    try {
      response = bulkMessageWriter.write(sensorType, configurations, messages);

      // Make sure all ids are included in the BulkWriterResponse
      ids.removeAll(response.getSuccesses());
      response.getErrors().values().forEach(ids::removeAll);
      response.addAllSuccesses(ids);

    } catch (Throwable e) {
      response.addAllErrors(e, ids);
    }

    long endTime = System.currentTimeMillis();
    long elapsed = endTime - startTime;
    LOG.debug("Flushed batch successfully; sensorType={}, batchSize={}, took={} ms", sensorType, CollectionUtils.size(ids), elapsed);
    return response;
  }

  /**
   * Apply flush policies to all sensors and flush if necessary.
   * @param bulkMessageWriter writer that will do the actual writing
   * @param configurations writer configurations
   */
  public void flushAll(BulkMessageWriter<MESSAGE_T> bulkMessageWriter,
                       WriterConfiguration configurations) {
    // Sensors are removed from the sensorTupleMap when flushed so we need to iterate over a copy of sensorTupleMap keys
    // to avoid a ConcurrentModificationException.
    for (String sensorType : new HashSet<>(sensorMessageCache.keySet())) {
      shouldFlush(sensorType, bulkMessageWriter, configurations, sensorMessageCache.get(sensorType));
    }
  }

  /**
   * Add a custom flush policy in addition to the default policies.
   * @param flushPolicy flush policy
   */
  public void addFlushPolicy(FlushPolicy flushPolicy) {
    this.flushPolicies.add(flushPolicy);
  }

  /**
   * Checks each flush policy to determine if a batch should be flushed.  A batch is flushed and the remaining policies
   * are skipped when a policy returns true.
   * @param sensorType sensor type
   * @param bulkMessageWriter writer that will do the actual writing
   * @param configurations writer configurations
   * @param messages messages to be written
   */
  private boolean shouldFlush(String sensorType, BulkMessageWriter<MESSAGE_T> bulkMessageWriter, WriterConfiguration configurations, List<BulkMessage<MESSAGE_T>> messages) {
    boolean flush = false;
    if (messages.size() > 0) { // no need to flush empty batches
      for(FlushPolicy<MESSAGE_T> flushPolicy: flushPolicies) {
        flush = flushPolicy.shouldFlush(sensorType, configurations, messages);
        if(flush) {
          break;
        }
      }
    }

    return flush;
  }

  private void preFlush( String sensorType, WriterConfiguration configurations, List<BulkMessage<MESSAGE_T>> messages) {
    for(FlushPolicy flushPolicy: flushPolicies) {
      flushPolicy.preFlush(sensorType, configurations, messages);
    }
  }

  /**
   * Called after a batch is flushed.
   *
   * The message cache is cleared and the {@link org.apache.metron.writer.FlushPolicy#postFlush(String, BulkWriterResponse)}
   * method is called for each flush policy.
   *
   * @param sensorType sensor type
   * @param response response from a bulk write call
   */
  private void postFlush(String sensorType, BulkWriterResponse response) {
    sensorMessageCache.remove(sensorType);
    for(FlushPolicy flushPolicy: flushPolicies) {
      flushPolicy.postFlush(sensorType, response);
    }
  }
}
