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
package org.apache.metron.elasticsearch.writer;

import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.json.simple.JSONObject;

/**
 * Generates the name of the search index that a given message
 * will be written to.
 */
public interface IndexNameGenerator {

  /**
   * Initialize the IndexNameGenerator.
   *
   * @param configurations The user-defined indexing configuration.
   */
  void init(WriterConfiguration configurations);

  /**
   * Returns the name of the index.
   *
   * @param message The message that needs indexed.
   * @param sensorType The type of sensor that generated the message.
   * @param configurations The user-defined indexing configuration.
   * @return The name of the index that the message should be written to.
   */
  String indexName(JSONObject message, String sensorType, WriterConfiguration configurations);

  /**
   * Returns the doc type.
   *
   * @param message The message that needs indexed.
   * @param sensorType The type of sensor that generated the message.
   * @param configurations The user-defined indexing configuration.
   * @return The doc type for the message.
   */
  String docType(JSONObject message, String sensorType, WriterConfiguration configurations);
}
