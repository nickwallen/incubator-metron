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
import org.apache.metron.common.system.Clock;
import org.apache.metron.elasticsearch.utils.ElasticsearchUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Generates the Elasticsearch index name based on when the message
 * is being processed.  The processing time is pulled from the system clock.
 */
public class ProcessingTimeIndexNameGenerator implements IndexNameGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Formats the date contained in the index name.
   */
  private SimpleDateFormat dateFormat;

  /**
   * A clock used to tell time.
   */
  private Clock clock;

  public ProcessingTimeIndexNameGenerator() {
    clock = new Clock();
  }

  @Override
  public void init(WriterConfiguration config) {
    dateFormat = ElasticsearchUtils.getIndexFormat(config.getGlobalConfig());
  }

  @Override
  public String indexName(JSONObject message, String sensorType, WriterConfiguration config) {

    // use system time when generating the index name
    long now = clock.currentTimeMillis();
    final String postfix = dateFormat.format(new Date(now));
    String indexName = ElasticsearchUtils.getIndexName(sensorType, postfix, config);

    LOG.debug("Using index named '{}' where sensorType={}, postfix={}, timestamp={}", indexName, sensorType, postfix, now);
    return indexName;
  }

  @Override
  public String docType(JSONObject message, String sensorType, WriterConfiguration configurations) {

    return sensorType + "_doc";
  }

  public ProcessingTimeIndexNameGenerator withClock(Clock clock) {
    this.clock = clock;
    return this;
  }

}
