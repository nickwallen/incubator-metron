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
package org.apache.metron.profiler.client.stellar;

import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.client.HBaseProfilerClientBuilder;
import org.apache.metron.profiler.client.ProfilerClient;
import org.apache.metron.stellar.dsl.BaseStellarFunction;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.ParseException;
import org.apache.metron.stellar.dsl.Stellar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.metron.stellar.dsl.Context.Capabilities.GLOBAL_CONFIG;

@Stellar(
        namespace="PROFILE",
        name="PUT",
        description="Writes a series of values to a stored profile in HBase.",
        params={
                "values - The profile measurement values to write.",
        },
        returns="The number of values successfully written."
)
public class PutProfile extends BaseStellarFunction {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private Context context;

  @Override
  public void initialize(Context context) {
    super.initialize(context);
    this.context = context;
  }

  @Override
  public Object apply(List<Object> args, Context context) throws ParseException {
    List<ProfileMeasurement> measurements = Util.getArg(0, List.class, args);
    ProfilerClient client = new HBaseProfilerClientBuilder()
            .withGlobals(globals(context))
            .build();
    int count = client.put(measurements);
    return count;
  }

  @Override
  public Object apply(List<Object> args) {
    return apply(args, this.context);
  }

  private Map<String, Object> globals(Context context) {
    Map<String, Object> globals = (Map<String, Object>) context
            .getCapability(GLOBAL_CONFIG)
            .orElse(new HashMap<>());
    return globals;
  }
}
