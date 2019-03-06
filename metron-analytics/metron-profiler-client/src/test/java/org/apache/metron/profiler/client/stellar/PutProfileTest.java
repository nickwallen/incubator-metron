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

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.metron.hbase.mock.MockHBaseTableProvider;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.client.HBaseProfilerClientBuilder;
import org.apache.metron.profiler.client.ProfilerClient;
import org.apache.metron.stellar.common.DefaultStellarStatefulExecutor;
import org.apache.metron.stellar.common.StellarStatefulExecutor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.functions.resolver.SimpleFunctionResolver;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.client.TestUtility.copy;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_COLUMN_FAMILY;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE_PROVIDER;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD_UNITS;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_SALT_DIVISOR;

public class PutProfileTest {

  private static final long periodDuration = 15;
  private static final TimeUnit periodUnits = TimeUnit.MINUTES;
  private static final int saltDivisor = 1000;
  private static final String tableName = "profiler";
  private static final String columnFamily = "P";
  private StellarStatefulExecutor executor;
  private Map<String, Object> state;
  private ProfilerClient profilerClient;

  private <T> T run(String expression, Class<T> clazz) {
    return executor.execute(expression, state, clazz);
  }

  @Before
  public void setup() {
    state = new HashMap<>();

    // global properties
    Map<String, Object> global = new HashMap<String, Object>() {{
      put(PROFILER_HBASE_TABLE.getKey(), tableName);
      put(PROFILER_COLUMN_FAMILY.getKey(), columnFamily);
      put(PROFILER_HBASE_TABLE_PROVIDER.getKey(), MockHBaseTableProvider.class.getName());
      put(PROFILER_PERIOD.getKey(), Long.toString(periodDuration));
      put(PROFILER_PERIOD_UNITS.getKey(), periodUnits.toString());
      put(PROFILER_SALT_DIVISOR.getKey(), Integer.toString(saltDivisor));
    }};

    final HTableInterface table = MockHBaseTableProvider.addToCache(tableName, columnFamily);
    profilerClient = new HBaseProfilerClientBuilder()
            .withGlobals(global)
            .withTable(table)
            .build();

    // create the stellar execution environment
    executor = new DefaultStellarStatefulExecutor(
            new SimpleFunctionResolver()
                    .withClass(PutProfile.class)
                    .withClass(GetProfile.class)
                    .withClass(FixedLookback.class),
            new Context.Builder()
                    .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
                    .build());
  }

  @Test
  public void shouldPutMeasurement() {
    // create some measurements to write
    final long startTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(3);
    final int count = 10;
    ProfileMeasurement m = new ProfileMeasurement()
            .withProfileName("profile1")
            .withEntity("entity1")
            .withPeriod(startTime, periodDuration, periodUnits);
    List<ProfileMeasurement> expected = copy(m, count, val -> 1);
    state.put("values", expected);

    // write the measurements
    int result = run("PROFILE_PUT(values)", Integer.class);
    Assert.assertEquals(count, result);

    // read back the measurements
    List<Integer> actuals = run("PROFILE_GET('profile1', 'entity1', PROFILE_FIXED(3, 'HOURS'))", List.class);
    Assert.assertEquals(count, actuals.size());
    for(int actual: actuals) {
      Assert.assertEquals(1, actual);
    }
  }
}
