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

package org.apache.metron.common.configuration.writer;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Configures a writer which indexes messages.
 *
 * <p>Each index destination (HDFS, Elasticsearch, Solr) has a separate writer configuration.
 * <p>Each writer allows separate configuration values for each sensor.
 */
public interface WriterConfiguration extends Serializable {

  /**
   * Returns the batch size for a given sensor.
   *
   * @param sensorName The name of the sensor.
   * @return The batch size for a given sensor.
   */
  int getBatchSize(String sensorName);

  /**
   * Returns the batch timeout for a given sensor.
   *
   * @param sensorName The name of the sensor.
   * @return The batch timeout for a given sensor.
   */
  int getBatchTimeout(String sensorName);

  /**
   * Returns the batch timeout for all configured sensors.
   *
   * @return All of the batch timeouts.
   */
  List<Integer> getAllConfiguredTimeouts();

  /**
   * Returns the index name for a given sensor.
   *
   * @param sensorName The name of the sensor.
   * @return The index name.
   */
  String getIndex(String sensorName);

  /**
   * Returns if this writer is enabled for a given sensor.
   *
   * @param sensorName The name of the sensor.
   * @return True, if this writer is enabled.  Otherwise, false.
   */
  boolean isEnabled(String sensorName);

  /**
   * Returns the configuration for the sensor.
   *
   * @param sensorName The name of the sensor.
   * @return
   */
  Map<String, Object> getSensorConfig(String sensorName);

  /**
   * Returns the global config.
   * @return The global configuration values.
   */
  Map<String, Object> getGlobalConfig();

  /**
   * Is the configuration for a given sensor, set to all default values?
   * @param sensorName The name of the sensor.
   * @return True, if the configuration contains only the default values.  Otherwise, false.
   */
  boolean isDefault(String sensorName);
}
