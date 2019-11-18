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
package org.apache.metron.enrichment.parallel;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import org.adrianwalker.multilinestring.Multiline;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.utils.JSONUtils;
import org.apache.metron.enrichment.adapters.stellar.StellarAdapter;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.json.simple.JSONObject;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelEnricherTest {

  private static ParallelEnricher enricher;
  private static Context stellarContext;
  private static AtomicInteger numAccesses = new AtomicInteger(0);
  private static Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType;
  // Declaring explicit class bc getClass().getSimpleName() returns "" for anon classes
  public static class DummyEnrichmentAdapter implements EnrichmentAdapter<CacheKey> {
    @Override
    public void logAccess(CacheKey value) {

    }

    @Override
    public JSONObject enrich(CacheKey value) {
      return null;
    }

    @Override
    public boolean initializeAdapter(Map<String, Object> config) {
      return false;
    }

    @Override
    public void updateAdapter(Map<String, Object> config) {

    }

    @Override
    public void cleanup() {

    }

    @Override
    public String getOutputPrefix(CacheKey value) {
      return null;
    }
  }

  // Declaring explicit class bc getClass().getSimpleName() returns "" for anon classes
  public static class AccessLoggingStellarAdapter extends StellarAdapter {
    @Override
    public void logAccess(CacheKey value) {
      numAccesses.incrementAndGet();
    }
  }

  @BeforeClass
  public static void setup() {
    ConcurrencyContext infrastructure = new ConcurrencyContext();
    infrastructure.initialize(5, 100, 10, null, null, false);
    stellarContext = new Context.Builder()
                         .build();
    StellarFunctions.initialize(stellarContext);
    StellarAdapter adapter = new AccessLoggingStellarAdapter().ofType("ENRICHMENT");
    adapter.initializeAdapter(new HashMap<>());
    EnrichmentAdapter<CacheKey> dummy = new DummyEnrichmentAdapter();

    enrichmentsByType = ImmutableMap.of("stellar", adapter, "dummy", dummy);
    enricher = new ParallelEnricher(enrichmentsByType, infrastructure, false);
  }

  /**
   *
   * {
   *   "enrichment": {
   *     "fieldMap": {
   *       "stellar": {
   *         "config": {
   *           "numeric": {
   *             "map": "{ 'blah' : 1}",
   *             "one": "MAP_GET('blah', map)",
   *             "foo": "1 + 1"
   *           },
   *           "ALL_CAPS": "TO_UPPER(source.type)"
   *         }
   *       }
   *     },
   *     "fieldToTypeMap": {}
   *   },
   *   "threatIntel": {}
   * }
   */
  @Multiline
  public static String goodConfig;

  @Test
  public void testCacheHit() throws Exception {
    numAccesses.set(0);
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, "test");
    }};
    for(int i = 0;i < 10;++i) {
      SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(goodConfig, SensorEnrichmentConfig.class);
      config.getConfiguration().putIfAbsent("stellarContext", stellarContext);
      ParallelEnricher.EnrichmentResult result = enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
    }
    //we only want 2 actual instances of the adapter.enrich being run due to the cache.
    Assert.assertTrue(2 >= numAccesses.get());
  }

  @Test
  public void testGoodConfig() throws Exception {
    SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(goodConfig, SensorEnrichmentConfig.class);
    config.getConfiguration().putIfAbsent("stellarContext", stellarContext);
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, "test");
    }};
    ParallelEnricher.EnrichmentResult result = enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
    JSONObject ret = result.getResult();
    Assert.assertEquals("Got the wrong result count: " + ret, 11, ret.size());
    Assert.assertEquals(1, ret.get("map.blah"));
    Assert.assertEquals("test", ret.get("source.type"));
    Assert.assertEquals(1, ret.get("one"));
    Assert.assertEquals(2, ret.get("foo"));
    Assert.assertEquals("TEST", ret.get("ALL_CAPS"));
    Assert.assertEquals(0, result.getEnrichmentErrors().size());
    Assert.assertTrue(result.getResult().containsKey("adapter.accessloggingstellaradapter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("adapter.accessloggingstellaradapter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.end.ts"));
  }

  /**
   * {
   *   "enrichment": {
   *     "fieldMap": {
   *       "dummy": [
   *         "notthere"
   *       ]
   *     },
   *     "fieldToTypeMap": {}
   *   },
   *   "threatIntel": {}
   * }
   */
  @Multiline
  public static String nullConfig;

  @Test
  public void testNullEnrichment() throws Exception {
    SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(nullConfig, SensorEnrichmentConfig.class);
    config.getConfiguration().putIfAbsent("stellarContext", stellarContext);
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, "test");
    }};
    ParallelEnricher.EnrichmentResult result = enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
    JSONObject ret = result.getResult();
    Assert.assertEquals("Got the wrong result count: " + ret, 7, ret.size());
    Assert.assertTrue(result.getResult().containsKey("adapter.dummyenrichmentadapter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("adapter.dummyenrichmentadapter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.end.ts"));
  }

  /**
   * {
   *   "enrichment": {
   *     "fieldMap": {
   *       "stellar": {
   *         "config": {
   *           "numeric": [
   *             "map := { 'blah' : 1}",
   *             "one := MAP_GET('blah', map)",
   *             "foo := 1 + 1"
   *           ],
   *           "ALL_CAPS": "TO_UPPER(source.type)",
   *           "errors": [
   *             "error := 1/0"
   *           ]
   *         }
   *       }
   *     },
   *     "fieldToTypeMap": {}
   *   },
   *   "threatIntel": {}
   * }
   */
  @Multiline
  public static String badConfig;

  @Test
  public void testBadConfig() throws Exception {
    SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(badConfig, SensorEnrichmentConfig.class);
    config.getConfiguration().putIfAbsent("stellarContext", stellarContext);
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, "test");
    }};
    ParallelEnricher.EnrichmentResult result = enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
    JSONObject ret = result.getResult();
    Assert.assertEquals(ret + " is not what I expected", 11, ret.size());
    Assert.assertEquals(1, ret.get("map.blah"));
    Assert.assertEquals("test", ret.get("source.type"));
    Assert.assertEquals(1, ret.get("one"));
    Assert.assertEquals(2, ret.get("foo"));
    Assert.assertEquals("TEST", ret.get("ALL_CAPS"));
    Assert.assertEquals(1, result.getEnrichmentErrors().size());
    Assert.assertTrue(result.getResult().containsKey("adapter.accessloggingstellaradapter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("adapter.accessloggingstellaradapter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.splitter.end.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.begin.ts"));
    Assert.assertTrue(result.getResult().containsKey("parallelenricher.enrich.end.ts"));
  }

  /**
   * {
   *   "enrichment": {
   *     "fieldMap": {
   *       "hbaseThreatIntel": [
   *         "ip_src_addr"
   *       ]
   *     },
   *     "fieldToTypeMap": {}
   *   },
   *   "threatIntel": {}
   * }
   */
  @Multiline
  public static String badConfigWrongEnrichmentType;

  @Test
  public void testBadConfigWrongEnrichmentType() throws Exception {
    SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(badConfigWrongEnrichmentType, SensorEnrichmentConfig.class);
    config.getConfiguration().putIfAbsent("stellarContext", stellarContext);
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, "test");
    }};
    try {
      enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
      Assert.fail("This is an invalid config, we should have failed.");
    }
    catch(IllegalStateException ise) {
      Assert.assertEquals(ise.getMessage()
              , "Unable to find an adapter for hbaseThreatIntel, possible adapters are: " + Joiner.on(",").join(enrichmentsByType.keySet())
      );
    }
  }

  /**
   * {
   *   "enrichment": {
   *     "fieldMap": {
   *       "stellar": {
   *         "config": {
   *           "block-1": {
   *             "block1-slow-enrichhment": "SLEEP(5000)",
   *             "block1-fast-enrichment": "2 + 2"
   *           },
   *           "block-2": {
   *              "block2-enrichment": "4 + 4"
   *           }
   *         }
   *       }
   *     },
   *     "fieldToTypeMap": {}
   *   },
   *   "threatIntel": {}
   * }
   */
  @Multiline
  public static String timeoutConfig;

  @Test
  public void testTimeoutExceeded() throws Exception {
    // the enrichments will sleep for 5 seconds, which exceeds the enrichment timeout
    SensorEnrichmentConfig config = JSONUtils.INSTANCE.load(timeoutConfig, SensorEnrichmentConfig.class);
    config.getConfiguration().putIfAbsent("stellarContext", stellarContext);

    // attempt to enrich the message which will exceed the enrichment timeout
    final String sourceType = "test";
    JSONObject message = new JSONObject() {{
      put(Constants.SENSOR_TYPE, sourceType);
    }};
    ParallelEnricher.EnrichmentResult result = enricher.apply(message, EnrichmentStrategies.ENRICHMENT, config, null);
    JSONObject enrichedMessage = result.getResult();

    // only 'block1' has a "slow" enrichment that will trigger a timeout. this causes all 'block1' enrichments to be
    // ignored because they cannot all be completed within the timeout
    Assert.assertFalse(enrichedMessage.containsKey("block1-fast-enrichment"));
    Assert.assertFalse(enrichedMessage.containsKey("block1-slow-enrichment"));

    // the enrichments in 'block2' are not affected by the timeout because they are in a separate block
    Assert.assertEquals(8, enrichedMessage.get("block2-enrichment"));

    // an enrichment error should indicate that a timeout occurred
    Assert.assertEquals(1, result.getEnrichmentErrors().size());

    Assert.assertEquals(sourceType, enrichedMessage.get("source.type"));
    Assert.assertTrue(enrichedMessage.containsKey("adapter.accessloggingstellaradapter.begin.ts"));
    Assert.assertTrue(enrichedMessage.containsKey("adapter.accessloggingstellaradapter.end.ts"));
    Assert.assertTrue(enrichedMessage.containsKey("parallelenricher.splitter.begin.ts"));
    Assert.assertTrue(enrichedMessage.containsKey("parallelenricher.splitter.end.ts"));
    Assert.assertTrue(enrichedMessage.containsKey("parallelenricher.enrich.begin.ts"));
    Assert.assertTrue(enrichedMessage.containsKey("parallelenricher.enrich.end.ts"));
  }

  @Test
  public void testTimeoutNotExceeded() throws Exception {
    Assert.fail("implement me");
  }
}
