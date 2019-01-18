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

package org.apache.metron.profiler.spark;

import org.adrianwalker.multilinestring.Multiline;
import org.apache.commons.io.FileUtils;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.hbase.mock.MockHBaseTableProvider;
import org.apache.metron.hbase.mock.MockHTable;
import org.apache.metron.integration.BaseIntegrationTest;
import org.apache.metron.integration.ComponentRunner;
import org.apache.metron.integration.UnableToStartException;
import org.apache.metron.integration.components.KafkaComponent;
import org.apache.metron.integration.components.ZKServerComponent;
import org.apache.metron.profiler.client.stellar.FixedLookback;
import org.apache.metron.profiler.client.stellar.GetProfile;
import org.apache.metron.profiler.client.stellar.WindowLookback;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.stellar.common.DefaultStellarStatefulExecutor;
import org.apache.metron.stellar.common.StellarStatefulExecutor;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.functions.resolver.SimpleFunctionResolver;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_COLUMN_FAMILY;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_HBASE_TABLE_PROVIDER;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD;
import static org.apache.metron.profiler.client.stellar.ProfilerClientConfig.PROFILER_PERIOD_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_COLUMN_FAMILY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_SALT_DIVISOR;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_TABLE_NAME;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_TABLE_PROVIDER;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG_UNITS;
import static org.junit.Assert.assertTrue;

/**
 * An integration test for the {@link StreamingProfiler}.
 */
public class StreamingProfilerIntegrationTest extends BaseIntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  /**
   * {
   *   "timestampField": "timestamp",
   *   "profiles": [
   *      {
   *        "profile": "count-by-ip",
   *        "foreach": "ip_src_addr",
   *        "init": { "count": 0 },
   *        "update": { "count" : "count + 1" },
   *        "result": "count"
   *      },
   *      {
   *        "profile": "total-count",
   *        "foreach": "'total'",
   *        "init": { "count": 0 },
   *        "update": { "count": "count + 1" },
   *        "result": "count"
   *      }
   *   ]
   * }
   */
  @Multiline
  private static String profileJson;
  private static SparkSession spark;
  private Properties profilerProperties;
  private Properties readerProperties;
  private Properties writerProperties;
  private StellarStatefulExecutor executor;
  private static ColumnBuilder columnBuilder;
  private static ZKServerComponent zkComponent;
  private static KafkaComponent kafkaComponent;
  private static ComponentRunner runner;
  private static MockHTable profilerTable;
  private static final String tableName = "profiler";
  private static final String columnFamily = "P";
  private static final int saltDivisor = 10;
  private static final String inputTopic = "indexing";
  private static final long windowLagMillis = TimeUnit.SECONDS.toMillis(5);
  private static final long windowDurationMillis = TimeUnit.SECONDS.toMillis(5);
  private static final long periodDurationMillis = TimeUnit.SECONDS.toMillis(30);
  private static final long profileTimeToLiveMillis = TimeUnit.SECONDS.toMillis(15);
  private static final long maxRoutesPerBolt = 100000;
  private static final long startAt = 10;
  private static final String entity = "10.0.0.1";

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @BeforeClass
  public static void setupSpark() {
    SparkConf conf = new SparkConf()
            .setMaster("local")
            .setAppName("StreamingProfilerIntegrationTest")
            .set("spark.sql.shuffle.partitions", "8");
    spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();
  }

  @AfterClass
  public static void tearDownSpark() {
    if(spark != null) {
      spark.close();
    }
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    MockHBaseTableProvider.clear();
    if (runner != null) {
      runner.stop();
    }
  }

  @Before
  public void setup() throws Exception {
    readerProperties = new Properties();
    writerProperties = new Properties();

    // the output will be written to a mock HBase table
    profilerProperties = new Properties();

    // TODO need to create separate StreamingProfilerConfig or something like that

    profilerProperties.put(PERIOD_DURATION.getKey(), Long.toString(periodDurationMillis));
    profilerProperties.put(PERIOD_DURATION_UNITS.getKey(), "MILLISECONDS");
    profilerProperties.put(HBASE_TABLE_PROVIDER.getKey(), MockHBaseTableProvider.class.getName());
    profilerProperties.put(HBASE_SALT_DIVISOR.getKey(), Integer.toString(saltDivisor));
    profilerProperties.put(HBASE_TABLE_NAME.getKey(), tableName);
    profilerProperties.put(HBASE_COLUMN_FAMILY.getKey(), columnFamily);

    // TODO are these needed?
//    profilerProperties.put("profiler.input.topic", inputTopic);
//    profilerProperties.put("profiler.ttl", Long.toString(profileTimeToLiveMillis));
//    profilerProperties.put("profiler.ttl.units", "MILLISECONDS");
//    profilerProperties.put("profiler.window.duration", Long.toString(windowDurationMillis));
//    profilerProperties.put("profiler.window.duration.units", "MILLISECONDS");
//    profilerProperties.put("profiler.window.lag", Long.toString(windowLagMillis));
//    profilerProperties.put("profiler.window.lag.units", "MILLISECONDS");
//    profilerProperties.put("kafka.start", "UNCOMMITTED_EARLIEST");
//    profilerProperties.put("kafka.security.protocol", "PLAINTEXT");

    // create the mock hbase table
    MockHBaseTableProvider.addToCache(tableName, columnFamily);

    // define the globals required by `PROFILE_GET`
    Map<String, Object> global = new HashMap<String, Object>() {{
      put(PROFILER_HBASE_TABLE.getKey(), tableName);
      put(PROFILER_COLUMN_FAMILY.getKey(), columnFamily);
      put(PROFILER_HBASE_TABLE_PROVIDER.getKey(), MockHBaseTableProvider.class.getName());
    }};

    // create the stellar execution environment
    executor = new DefaultStellarStatefulExecutor(
            new SimpleFunctionResolver()
                    .withClass(GetProfile.class)
                    .withClass(FixedLookback.class)
                    .withClass(WindowLookback.class),
            new Context.Builder()
                    .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
                    .build());

    zkComponent = getZKServerComponent(profilerProperties);

    // create the input topic
    kafkaComponent = getKafkaComponent(profilerProperties, Arrays.asList(
            new KafkaComponent.Topic(inputTopic, 1)));

    // start all components
    runner = new ComponentRunner.Builder()
            .withComponent("zk", zkComponent)
            .withComponent("kafka", kafkaComponent)
            .withMillisecondsBetweenAttempts(15000)
            .withNumRetries(10)
            .withCustomShutdownOrder(new String[] {"kafka","zk"})
            .build();
    runner.start();
  }

  /**
   * This test uses the Streaming Profiler to seed two profiles using archived telemetry.
   *
   * The first profile counts the number of messages by 'ip_src_addr'.  The second profile counts the total number
   * of messages.
   *
   * The archived telemetry contains timestamps from around July 7, 2018.  All of the measurements
   * produced will center around this date.
   */
  @Test
  public void testStreamingProfiler() throws Exception {

    // TODO configure writer properties

    // write the telemetry to the Kafka topic
    List<String> telemetry = FileUtils.readLines(new File("/Users/nallen/tmp/indexed/json/bro/enrichment-hdfsIndexingBolt-3-0-1530978700434.json"));
    kafkaComponent.writeMessages(inputTopic, telemetry);

    // setup Kafka
    profilerProperties.put(TELEMETRY_INPUT_FORMAT.getKey(), "kafka");
    readerProperties.put("subscribe", inputTopic);
    readerProperties.put("kafka.bootstrap.servers", kafkaComponent.getBrokerList());
    readerProperties.put("startingOffsets", "earliest");

    // setup profiler
    profilerProperties.put(PERIOD_DURATION.getKey(), 30);
    profilerProperties.put(PERIOD_DURATION_UNITS.getKey(), "seconds");
    profilerProperties.put(WINDOW_LAG.getKey(), 5);
    profilerProperties.put(WINDOW_LAG_UNITS.getKey(), "seconds");

    StreamingProfiler profiler = new StreamingProfiler();
    profiler.run(spark.sparkContext().getConf(), profilerProperties, getGlobals(), readerProperties, writerProperties, getProfile());

    validateProfiles();
  }

  /**
   * Validates the profiles that were built.
   *
   * These tests use the Batch Profiler to seed two profiles with archived telemetry.  The first profile
   * called 'count-by-ip', counts the number of messages by 'ip_src_addr'.  The second profile called
   * 'total-count', counts the total number of messages.
   */
  private void validateProfiles() {
    // the max timestamp in the data is around July 7, 2018
    assign("maxTimestamp", "1530978728982L");

    // the 'window' looks up to 5 hours before the max timestamp
    assign("window", "PROFILE_WINDOW('from 5 hours ago', maxTimestamp)");

    // there are 26 messages where ip_src_addr = 192.168.66.1
    assertTrue(execute("[26] == PROFILE_GET('count-by-ip', '192.168.66.1', window)", Boolean.class));

    // there are 74 messages where ip_src_addr = 192.168.138.158
    assertTrue(execute("[74] == PROFILE_GET('count-by-ip', '192.168.138.158', window)", Boolean.class));

    // there are 100 messages in all
    assertTrue(execute("[100] == PROFILE_GET('total-count', 'total', window)", Boolean.class));
  }

  private ProfilerConfig getProfile() throws IOException {
    return ProfilerConfig.fromJSON(profileJson);
  }

  private Properties getGlobals() {
    return new Properties();
  }

  /**
   * Assign a value to the result of an expression.
   *
   * @param var The variable to assign.
   * @param expression The expression to execute.
   */
  private void assign(String var, String expression) {
    executor.assign(var, expression, Collections.emptyMap());
  }

  /**
   * Execute a Stellar expression.
   *
   * @param expression The Stellar expression to execute.
   * @param clazz
   * @param <T>
   * @return The result of executing the Stellar expression.
   */
  private <T> T execute(String expression, Class<T> clazz) {
    T results = executor.execute(expression, Collections.emptyMap(), clazz);
    LOG.debug("{} = {}", expression, results);
    return results;
  }
}
