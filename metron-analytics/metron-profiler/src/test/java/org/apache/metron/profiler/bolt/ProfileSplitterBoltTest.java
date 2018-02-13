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

package org.apache.metron.profiler.bolt;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.test.bolt.BaseBoltTest;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the ProfileSplitterBolt.
 */
public class ProfileSplitterBoltTest extends BaseBoltTest {

  /**
   * {
   *   "ip_src_addr": "10.0.0.1",
   *   "ip_dst_addr": "10.0.0.20",
   *   "protocol": "HTTP",
   *   "timestamp": 1111111111111,
   *   "custom.timestamp": 2222222222222,
   *   "timestamp.string": "3333333333333"
   * }
   */
  @Multiline
  private String input;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "onlyif": "protocol == 'HTTP'",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ]
   * }
   */
  @Multiline
  private String profileWithOnlyIfTrue;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "onlyif": "false",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ]
   * }
   */
  @Multiline
  private String profileWithOnlyIfFalse;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ]
   * }
   */
  @Multiline
  private String profileWithOnlyIfMissing;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "onlyif": "NOT-VALID",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ]
   * }
   */
  @Multiline
  private String profileWithOnlyIfInvalid;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ],
   *   "timestampField": "custom.timestamp"
   * }
   */
  @Multiline
  private String profileUsingCustomTimestampField;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ],
   *   "timestampField": "missing.timestamp"
   * }
   */
  @Multiline
  private String profileUsingMissingTimestampField;

  /**
   * {
   *   "profiles": [
   *      {
   *        "profile": "test",
   *        "foreach": "ip_src_addr",
   *        "init": {},
   *        "update": {},
   *        "result": "2"
   *      }
   *   ],
   *   "timestampField": "timestamp.string"
   * }
   */
  @Multiline
  private String profileUsingStringTimestampField;

  private JSONObject message;

  @Before
  public void setup() throws ParseException {

    // parse the input message
    JSONParser parser = new JSONParser();
    message = (JSONObject) parser.parse(input);

    // ensure the tuple returns the expected json message
    when(tuple.getBinary(0)).thenReturn(input.getBytes());
  }

  /**
   * Creates a ProfilerConfig based on a string containing JSON.
   *
   * @param configAsJSON The config as JSON.
   * @return The ProfilerConfig.
   * @throws Exception
   */
  private ProfilerConfig toProfilerConfig(String configAsJSON) throws Exception {
    InputStream in = new ByteArrayInputStream(configAsJSON.getBytes("UTF-8"));
    return JSONUtils.INSTANCE.load(in, ProfilerConfig.class);
  }

  /**
   * Create a ProfileSplitterBolt to test
   */
  private ProfileSplitterBolt createBolt(ProfilerConfig config) throws Exception {

    ProfileSplitterBolt bolt = new ProfileSplitterBolt("zookeeperURL");
    bolt.setCuratorFramework(client);
    bolt.setZKCache(cache);
    bolt.getConfigurations().updateProfilerConfig(config);
    bolt.prepare(new HashMap<>(), topologyContext, outputCollector);

    return bolt;
  }

  /**
   * What happens when a profile's 'onlyif' expression is true?  The message
   * should be applied to the profile.
   */
  @Test
  public void testOnlyIfTrue() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfTrue);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), any(Values.class));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * All messages are applied to a profile where 'onlyif' is missing.  A profile with no
   * 'onlyif' is treated the same as if 'onlyif=true'.
   */
  @Test
  public void testOnlyIfMissing() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfMissing);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), any(Values.class));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * What happens when a profile's 'onlyif' expression is false?  The message
   * should NOT be applied to the profile.
   */
  @Test
  public void testOnlyIfFalse() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfFalse);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // a tuple should NOT be emitted for the downstream profile builder
    verify(outputCollector, times(0))
            .emit(any());

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * The entity associated with a ProfileMeasurement can be defined using a variable that is resolved
   * via Stella.  In this case the entity is defined as 'ip_src_addr' which is resolved to
   * '10.0.0.1' based on the data contained within the message.
   */
  @Test
  public void testResolveEntityName() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfTrue);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // expected values
    String expectedEntity = "10.0.0.1";
    ProfileConfig expectedConfig = config.getProfiles().get(0);
    long expectedTimestamp = 1111111111111L;
    Values expected = new Values(expectedEntity, expectedConfig, message, expectedTimestamp);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), eq(expected));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * What happens when invalid Stella code is used for 'onlyif'?  The invalid profile should be ignored.
   */
  @Test
  public void testOnlyIfInvalid() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfInvalid);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // a tuple should NOT be emitted for the downstream profile builder
    verify(outputCollector, times(0))
            .emit(any(Values.class));
  }

  /**
   * If the profile configuration does not define a custom 'timestampField', a default
   * field should be used; 'timestamp'.
   */
  @Test
  public void testDefaultTimestampField() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileWithOnlyIfTrue);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // expected values
    String expectedEntity = "10.0.0.1";
    ProfileConfig expectedConfig = config.getProfiles().get(0);
    long expectedTimestamp = 1111111111111L;
    Values expected = new Values(expectedEntity, expectedConfig, message, expectedTimestamp);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), eq(expected));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * If a custom timestamp field is defined in the profiler configuration, the timestamp
   * is extracted from that field.
   */
  @Test
  public void testCustomTimestampField() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileUsingCustomTimestampField);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // expected values
    String expectedEntity = "10.0.0.1";
    ProfileConfig expectedConfig = config.getProfiles().get(0);
    long expectedTimestamp = 2222222222222L;
    Values expected = new Values(expectedEntity, expectedConfig, message, expectedTimestamp);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), eq(expected));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

  /**
   * If a message does not contain the timestamp field, it should not be emitted.  Messages that
   * are missing timestamps must be ignored.
   */
  @Test
  public void testMissingTimestampField() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileUsingMissingTimestampField);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // a tuple should NOT be emitted for the downstream profile builder
    verify(outputCollector, times(0))
            .emit(any());
  }

  /**
   * If a timestamp field contains a string, it should be converted to a long and treated
   * as epoch milliseconds.
   */
  @Test
  public void testStringTimestampField() throws Exception {

    ProfilerConfig config = toProfilerConfig(profileUsingStringTimestampField);
    ProfileSplitterBolt bolt = createBolt(config);
    bolt.execute(tuple);

    // expected values
    String expectedEntity = "10.0.0.1";
    ProfileConfig expectedConfig = config.getProfiles().get(0);
    long expectedTimestamp = 3333333333333L;
    Values expected = new Values(expectedEntity, expectedConfig, message, expectedTimestamp);

    // a tuple should be emitted for the downstream profile builder
    verify(outputCollector, times(1))
            .emit(eq(tuple), eq(expected));

    // the original tuple should be ack'd
    verify(outputCollector, times(1))
            .ack(eq(tuple));
  }

}
