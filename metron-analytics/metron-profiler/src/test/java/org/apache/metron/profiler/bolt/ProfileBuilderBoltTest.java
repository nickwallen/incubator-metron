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
import org.apache.log4j.Level;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.profiler.ProfileBuilder;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.stellar.StellarExecutor;
import org.apache.metron.test.bolt.BaseBoltTest;
import org.apache.metron.test.utils.UnitTestHelper;
import org.apache.storm.Constants;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.refEq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the ProfileBuilderBolt.
 */
public class ProfileBuilderBoltTest extends BaseBoltTest {

  /**
   * {
   *   "ip_src_addr": "10.0.0.1",
   *   "ip_dst_addr": "10.0.0.20"
   * }
   */
  @Multiline
  private String input;

  private JSONObject message;


  // TODO test initializes a ProfileBuilder

  // TODO test that different profiles have different builders

  // TODO test that different entities with same profile have different builders



  public static Tuple mockTickTuple() {
    return mockTuple(Constants.SYSTEM_COMPONENT_ID, Constants.SYSTEM_TICK_STREAM_ID);
  }

  public static Tuple mockTuple(String componentId, String streamId) {
    Tuple tuple = mock(Tuple.class);
    when(tuple.getSourceComponent()).thenReturn(componentId);
    when(tuple.getSourceStreamId()).thenReturn(streamId);
    return tuple;
  }

  public void setup(String profile) throws Exception {

    // parse the input message
    JSONParser parser = new JSONParser();
    message = (JSONObject) parser.parse(input);

    // the tuple will contain the original message
    when(tuple.getValueByField(eq("message"))).thenReturn(message);

    // the tuple will contain the 'fully resolved' name of the entity
    when(tuple.getValueByField(eq("entity"))).thenReturn("10.0.0.1");

    // the tuple will contain the profile definition
    ProfileConfig profileConfig = JSONUtils.INSTANCE.load(profile, ProfileConfig.class);
    when(tuple.getValueByField(eq("profile"))).thenReturn(profileConfig);
  }

  /**
   * Create a ProfileBuilderBolt to test
   */
  private ProfileBuilderBolt createBolt() throws IOException {

    ProfileBuilderBolt bolt = new ProfileBuilderBolt("zookeeperURL");
    bolt.setCuratorFramework(client);
    bolt.setTreeCache(cache);
    bolt.withPeriodDuration(10, TimeUnit.MINUTES);
    bolt.withTimeToLive(30, TimeUnit.MINUTES);
    bolt.prepare(new HashMap<>(), topologyContext, outputCollector);

    return bolt;
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "onlyif": "true",
   *   "init": {
   *     "x": "10",
   *     "y": "20"
   *   },
   *   "update": {
   *     "x": "x + 10",
   *     "y": "y + 20"
   *   },
   *   "result": "x + y"
   * }
   */
  @Multiline
  private String basicProfile;

  /**
   * Ensure that the bolt can update a profile based on new messages that it receives.
   */
  @Test
  public void testProfileUpdate() throws Exception {

    setup(basicProfile);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);

    // validate that x=10+10+10 y=20+20+20
    ProfileBuilder builder = bolt.getBuilder(tuple);
    assertEquals(10+10+10.0, builder.valueOf("x"));
    assertEquals(20+20+20.0, builder.valueOf("y"));
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "update": { "x": "2" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileWithNoInit;

  /**
   * If the 'init' field is not defined, then the profile should
   * behave as normal, but with no variable initialization.
   */
  @Test
  public void testProfileWithNoInit() throws Exception {

    setup(profileWithNoInit);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);

    // validate
    ProfileBuilder builder = bolt.getBuilder(tuple);
    assertEquals(2, builder.valueOf("x"));
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "init": { "x": "2" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileWithNoUpdate;

  /**
   * If the 'update' field is not defined, then no updates should occur as messages
   * are received.
   */
  @Test
  public void testProfileWithNoUpdate() throws Exception {

    setup(profileWithNoUpdate);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);
    bolt.execute(tuple);

    // validate
    ProfileBuilder builder = bolt.getBuilder(tuple);
    assertEquals(2, builder.valueOf("x"));
  }

  /**
   * Ensure that the bolt can flush the profile when a tick tuple is received.
   */
  @Test
  public void testProfileFlush() throws Exception {

    // setup
    setup(basicProfile);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);

    // execute - the tick tuple triggers a flush of the profile
    bolt.execute(mockTickTuple());

    // capture the ProfileMeasurement that should be emitted
    ArgumentCaptor<Values> arg = ArgumentCaptor.forClass(Values.class);
    verify(outputCollector, times(1)).emit(arg.capture());

    Values actual = arg.getValue();
    ProfileMeasurement measurement = (ProfileMeasurement) actual.get(0);

    // verify
    assertThat(measurement.getValue(), equalTo(90.0));
    assertThat(measurement.getEntity(), equalTo("10.0.0.1"));
    assertThat(measurement.getProfileName(), equalTo("test"));
  }

  /**
   * What happens if we try to flush, but have yet to receive any messages to
   * apply to the profile?
   *
   * The ProfileBuilderBolt will not have received the data necessary from the
   * ProfileSplitterBolt, like the entity and profile name, that is required
   * to perform the flush.  The flush has to be skipped until this information
   * is received from the Splitter.
   */
  @Test
  public void testProfileFlushWithNoMessages() throws Exception {

    setup(basicProfile);
    ProfileBuilderBolt bolt = createBolt();

    // no messages have been received before a flush occurs
    bolt.execute(mockTickTuple());
    bolt.execute(mockTickTuple());
    bolt.execute(mockTickTuple());

    // no ProfileMeasurement should be written to the ProfileStore
    verify(outputCollector, times(0)).emit(any(Values.class));
  }

  /**
   * The executor's state should be cleared after a flush.
   */
  @Test
  public void testStateClearedAfterFlush() throws Exception {

    setup(basicProfile);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);

    // before the flush - the profile contains some state
    ProfileBuilder builder = bolt.getBuilder(tuple);
    assertNotNull(builder.valueOf("x"));
    assertNotNull(builder.valueOf("y"));

    // execute - should clear state from previous tuples
    bolt.execute(mockTickTuple());

    // after the flush - the profile should not have any state
    assertNull(builder.valueOf("x"));
    assertNull(builder.valueOf("y"));
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "onlyif": "true",
   *   "init":   { "x": 10 },
   *   "update": { "x": "x + 'string'" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileWithBadUpdate;

  /**
   * What happens when the profile contains a bad Stellar expression?
   */
  @Test
  public void testProfileWithBadUpdate() throws Exception {

    // setup - ensure the bad profile is used
    setup(profileWithBadUpdate);
    UnitTestHelper.setLog4jLevel(ProfileBuilderBolt.class, Level.FATAL);
    // execute
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    UnitTestHelper.setLog4jLevel(ProfileBuilderBolt.class, Level.ERROR);

    // verify - expect the tuple to be acked and an error reported
    verify(outputCollector, times(1)).ack(eq(tuple));
    verify(outputCollector, times(1)).reportError(any());
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "onlyif": "true",
   *   "init":   { "x": "10 + 'string'" },
   *   "update": { "x": "x + 2" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileWithBadInit;

  /**
   * What happens when the profile contains a bad Stellar expression?
   */
  @Test
  public void testProfileWithBadInit() throws Exception {

    // setup - ensure the bad profile is used
    setup(profileWithBadInit);

    // execute
    ProfileBuilderBolt bolt = createBolt();
    UnitTestHelper.setLog4jLevel(ProfileBuilderBolt.class, Level.FATAL);
    bolt.execute(tuple);
    UnitTestHelper.setLog4jLevel(ProfileBuilderBolt.class, Level.ERROR);
    // verify - expect the tuple to be acked and an error reported
    verify(outputCollector, times(1)).ack(eq(tuple));
    verify(outputCollector, times(1)).reportError(any());
  }

  /**
   * {
   *   "profile": "test",
   *   "foreach": "ip_src_addr",
   *   "onlyif": "true",
   *   "groupBy": ["2 + 2", "4 + 4"],
   *   "init":   { "x": "0" },
   *   "update": { "x": "x + 1" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileWithGroupBy;

  /**
   * Ensure that the Profile's 'groupBy' are handled correctly.
   */
  @Test
  public void testProfileWithGroupBy() throws Exception {

    // setup
    setup(profileWithGroupBy);
    ProfileBuilderBolt bolt = createBolt();
    bolt.execute(tuple);
    bolt.execute(tuple);

    // execute - the tick tuple triggers a flush of the profile
    bolt.execute(mockTickTuple());

    // capture the ProfileMeasurement that should be emitted
    ArgumentCaptor<Values> arg = ArgumentCaptor.forClass(Values.class);
    verify(outputCollector, times(1)).emit(arg.capture());

    Values actual = arg.getValue();
    ProfileMeasurement measurement = (ProfileMeasurement) actual.get(0);

    // verify the groups
    assertThat(measurement.getGroups().size(), equalTo(2));
    assertThat(measurement.getGroups().get(0), equalTo(4.0));
    assertThat(measurement.getGroups().get(1), equalTo(8.0));
  }
}
