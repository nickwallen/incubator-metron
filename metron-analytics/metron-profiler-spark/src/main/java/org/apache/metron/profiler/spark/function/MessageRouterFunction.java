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
package org.apache.metron.profiler.spark.function;

import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.profiler.DefaultMessageRouter;
import org.apache.metron.profiler.MessageRoute;
import org.apache.metron.profiler.MessageRouter;
import org.apache.metron.stellar.dsl.Context;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Responsible for finding routes for a given message.
 */
public class MessageRouterFunction implements FlatMapFunction<String, MessageRoute> {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private Map<String, String> globals;
  private ProfilerConfig profilerConfig;

  public MessageRouterFunction(ProfilerConfig profilerConfig, Map<String, String> globals) {
    this.profilerConfig = profilerConfig;
    this.globals = globals;
  }

  /**
   * Find all routes for a given telemetry message.
   *
   * <p>A message may need routed to multiple profiles should it be needed by more than one.  A
   * message may also not be routed should it not be needed by any profiles.
   *
   * @param jsonMessage The raw JSON message.
   * @return A list of message routes.
   */
  @Override
  public Iterator<MessageRoute> call(String jsonMessage) throws Exception {
    List<MessageRoute> routes;

    JSONParser parser = new JSONParser();
    Context context = TaskUtils.getContext(globals);
    MessageRouter router = new DefaultMessageRouter(context);

    // parse the raw message
    Optional<JSONObject> message = toMessage(jsonMessage, parser);
    if(message.isPresent()) {

      // find all routes
      routes = router.route(message.get(), profilerConfig, context);
      LOG.trace("Found {} route(s) for a message", routes.size());

    } else {
      // the message is not valid and must be ignored
      routes = Collections.emptyList();
      LOG.trace("No route possible. Unable to parse message.");
    }

    return routes.iterator();
  }

  /**
   * Parses the raw JSON of a message.
   *
   * @param json The raw JSON to parse.
   * @param parser The parser to use.
   * @return The parsed telemetry message.
   */
  private static Optional<JSONObject> toMessage(String json, JSONParser parser) {
    try {
      JSONObject message = (JSONObject) parser.parse(json);
      return Optional.of(message);

    } catch(Throwable e) {
      LOG.warn(String.format("Unable to parse message, message will be ignored; message='%s'", json), e);
      return Optional.empty();
    }
  }
}
