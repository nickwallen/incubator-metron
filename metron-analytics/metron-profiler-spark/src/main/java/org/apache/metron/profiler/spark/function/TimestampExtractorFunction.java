/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.metron.profiler.spark.function;

import org.apache.commons.lang3.ClassUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;

public class TimestampExtractorFunction implements MapFunction<String, Tuple2<JSONObject, Timestamp>> {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private String timestampField;
  private JSONParser parser;

  public TimestampExtractorFunction(String timestampField) {
    this.timestampField = timestampField;
    this.parser = new JSONParser();
  }

  @Override
  public Tuple2<JSONObject, Timestamp> call(String json) throws Exception {
    Timestamp timestamp = null;
    JSONObject message = null;

    try {
      message = (JSONObject) parser.parse(json);

      if(message.containsKey(timestampField)) {
        Object fieldValue = message.get(timestampField);
        if(fieldValue instanceof Long) {
          timestamp = new Timestamp((Long) fieldValue);

        } else if(LOG.isDebugEnabled()) {
          LOG.debug("Timestamp field contains unexpected type, expected Long; got {}; timestampField={}",
                  ClassUtils.getSimpleName(fieldValue, "null"), timestampField);
        }

      } else if(LOG.isDebugEnabled()) {
        LOG.debug("Message missing timestamp field; timestampField={}", timestampField);
      }

    } catch(Throwable e) {
      LOG.warn(String.format("Unable to parse message, message will be ignored"), e);
    }

    Tuple2<JSONObject, Timestamp> result = null;
    if(message != null && timestamp != null) {
      result = new Tuple2<>(message, timestamp);
    }

    return result;
  }
}
