/*
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
package org.apache.metron.profiler.storm;

import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.invoke.MethodHandles;

/**
 * Emits {@link ProfileMeasurement} values to a stream that will persist
 * the measurements in HBase using Phoenix.
 */
public class PhoenixEmitter implements ProfileMeasurementEmitter, Serializable {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String stream = "phoenix";

  @Override
  public String getStreamId() {
    return stream;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream(getStreamId(), new Fields("measurement"));
  }

  @Override
  public void emit(ProfileMeasurement measurement, OutputCollector collector) {
    collector.emit(getStreamId(), new Values(measurement));
    LOG.debug("Emitted measurement; stream={}, profile={}, entity={}, period={}, start={}, end={}",
            getStreamId(),
            measurement.getProfileName(),
            measurement.getEntity(),
            measurement.getPeriod().getPeriod(),
            measurement.getPeriod().getStartTimeMillis(),
            measurement.getPeriod().getEndTimeMillis());
  }
}
