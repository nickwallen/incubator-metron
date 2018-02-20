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

package org.apache.metron.profiler;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.stellar.dsl.Context;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DefaultMessageDistributorTest {

  /**
   * {
   *   "ip_src_addr": "10.0.0.1",
   *   "value": "22"
   * }
   */
  @Multiline
  private String inputOne;
  private JSONObject messageOne;

  /**
   * {
   *   "ip_src_addr": "10.0.0.2",
   *   "value": "22"
   * }
   */
  @Multiline
  private String inputTwo;
  private JSONObject messageTwo;

  /**
   * {
   *   "profile": "profile-one",
   *   "foreach": "ip_src_addr",
   *   "init":   { "x": "0" },
   *   "update": { "x": "x + 1" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileOne;

  /**
   * {
   *   "profile": "profile-two",
   *   "foreach": "ip_src_addr",
   *   "init":   { "x": "0" },
   *   "update": { "x": "x + 1" },
   *   "result": "x"
   * }
   */
  @Multiline
  private String profileTwo;

  private DefaultMessageDistributor distributor;
  private Context context;

  @Before
  public void setup() throws Exception {
    context = Context.EMPTY_CONTEXT();
    JSONParser parser = new JSONParser();
    messageOne = (JSONObject) parser.parse(inputOne);
    messageTwo = (JSONObject) parser.parse(inputTwo);

    long periodDurationMillis = TimeUnit.MINUTES.toMillis(15);
    long profileTimeToLiveMillis = TimeUnit.MINUTES.toMillis(30);
    long maxNumberOfRoutes = Long.MAX_VALUE;
    distributor = new DefaultMessageDistributor(
            periodDurationMillis,
            profileTimeToLiveMillis,
            maxNumberOfRoutes);
  }

  /**
   * Creates a profile definition based on a string of JSON.
   * @param json The string of JSON.
   */
  private ProfileConfig createDefinition(String json) throws IOException {
    return JSONUtils.INSTANCE.load(json, ProfileConfig.class);
  }

  /**
   * Tests that one message can be distributed to one profile.
   */
  @Test
  public void testDistribute() throws Exception {

    // setup
    long timestamp = 100;
    ProfileConfig definition = createDefinition(profileOne);
    String entity = (String) messageOne.get("ip_src_addr");
    MessageRoute route = new MessageRoute(definition, entity);

    // distribute one message and flush
    distributor.distribute(messageOne, timestamp, route, context);
    List<ProfileMeasurement> measurements = distributor.flush();

    // expect one measurement coming from one profile
    assertEquals(1, measurements.size());
    ProfileMeasurement m = measurements.get(0);
    assertEquals(definition.getProfile(), m.getProfileName());
    assertEquals(entity, m.getEntity());
  }

  @Test
  public void testDistributeWithTwoProfiles() throws Exception {

    // setup
    long timestamp = 100;
    String entity = (String) messageOne.get("ip_src_addr");

    // distribute one message to the first profile
    MessageRoute routeOne = new MessageRoute(createDefinition(profileOne), entity);
    distributor.distribute(messageOne, timestamp, routeOne, context);

    // distribute another message to the second profile, but same entity
    MessageRoute routeTwo = new MessageRoute(createDefinition(profileTwo), entity);
    distributor.distribute(messageOne, timestamp, routeTwo, context);

    // expect 2 measurements; 1 for each profile
    List<ProfileMeasurement> measurements = distributor.flush();
    assertEquals(2, measurements.size());
  }

  @Test
  public void testDistributeWithTwoEntities() throws Exception {

    // setup
    long timestamp = 100;

    // distribute one message
    String entityOne = (String) messageOne.get("ip_src_addr");
    MessageRoute routeOne = new MessageRoute(createDefinition(profileOne), entityOne);
    distributor.distribute(messageOne, timestamp, routeOne, context);

    // distribute another message with a different entity
    String entityTwo = (String) messageTwo.get("ip_src_addr");
    MessageRoute routeTwo =  new MessageRoute(createDefinition(profileTwo), entityTwo);
    distributor.distribute(messageTwo, timestamp, routeTwo, context);

    // expect 2 measurements; 1 for each entity
    List<ProfileMeasurement> measurements = distributor.flush();
    assertEquals(2, measurements.size());
  }

}
