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
package org.apache.metron.profiler.spark;

import org.apache.commons.collections4.IteratorUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.hbase.HTableProvider;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.profiler.DefaultMessageDistributor;
import org.apache.metron.profiler.DefaultMessageRouter;
import org.apache.metron.profiler.MessageDistributor;
import org.apache.metron.profiler.MessageRoute;
import org.apache.metron.profiler.MessageRouter;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.ProfilePeriod;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.apache.metron.profiler.hbase.SaltyRowKeyBuilder;
import org.apache.metron.profiler.hbase.ValueOnlyColumnBuilder;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_COLUMN_FAMILY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_SALT_DIVISOR;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_TABLE_NAME;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_WRITE_DURABILITY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_PATH;
import static org.apache.spark.sql.functions.sum;

/**
 * A Profiler that generates profiles in batch from archived telemetry.
 *
 * <p>The Batch Profiler is executed in Spark.
 */
public class BatchProfiler implements Serializable {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Execute the Batch Profiler.
   *
   * @param properties
   * @param profilerConfig
   * @return The number of profile measurements produced.
   */
  public long execute(Properties properties, ProfilerConfig profilerConfig) {
    LOG.debug("Building {} profile(s)", profilerConfig.getProfiles().size());

    // declarations that only exist here to make the code more readable below
    FlatMapFunction<String, MessageRoute> findRoutesFn;
    MapGroupsFunction<String, MessageRoute, ProfileMeasurementAdapter> buildProfilesFn;
    MapFunction<MessageRoute, String> groupByPeriodFn;
    MapPartitionsFunction<ProfileMeasurementAdapter, Integer> writeToHBaseFn;

    // required configuration values
    TimeUnit periodDurationUnits = TimeUnit.valueOf(PERIOD_DURATION_UNITS.get(properties, String.class));
    int periodDuration = PERIOD_DURATION.get(properties, Integer.class);
    long periodDurationMillis = periodDurationUnits.toMillis(periodDuration);
    int saltDivisor = HBASE_SALT_DIVISOR.get(properties, Integer.class);
    String tableName = HBASE_TABLE_NAME.get(properties, String.class);
    String columnFamily = HBASE_COLUMN_FAMILY.get(properties, String.class);
    String inputFormat = TELEMETRY_INPUT_FORMAT.get(properties, String.class);
    String inputPath = TELEMETRY_INPUT_PATH.get(properties, String.class);
    Durability durability = HBASE_WRITE_DURABILITY.get(properties, Durability.class);

    SparkSession spark = SparkSession
            .builder()
            .config(new SparkConf())
            .getOrCreate();

    // fetch the archived telemetry
    LOG.debug("Loading telemetry from '{}'", inputPath);
    Dataset<String> telemetry = spark
            .read()
            .format(inputFormat)
            .load(inputPath)
            .as(Encoders.STRING());
    LOG.debug("Found {} telemetry record(s)", telemetry.cache().count());

    // find all routes for each message
    findRoutesFn = msg -> findRoutes(msg, profilerConfig).iterator();
    Dataset<MessageRoute> routes = telemetry.flatMap(findRoutesFn, Encoders.bean(MessageRoute.class));
    LOG.debug("Generated {} message route(s)", routes.cache().count());

    // build the profiles
    groupByPeriodFn = route -> groupByKey(route, periodDuration, periodDurationUnits);
    buildProfilesFn = (grp, routesInGroup) -> buildProfile(routesInGroup, periodDurationMillis);
    Dataset<ProfileMeasurementAdapter> measurements = routes
            .groupByKey(groupByPeriodFn, Encoders.STRING())
            .mapGroups(buildProfilesFn, Encoders.bean(ProfileMeasurementAdapter.class));
    LOG.debug("Produced {} profile measurement(s)", measurements.cache().count());

    // write the profile measurements to HBase
    writeToHBaseFn = iter -> writeToHBase(iter, tableName, columnFamily, periodDurationMillis, saltDivisor, durability);
    long count = measurements
            .mapPartitions(writeToHBaseFn, Encoders.INT())
            .agg(sum("value"))
            .head()
            .getLong(0);
    LOG.debug("{} profile measurement(s) written to HBase", count);
    return count;
  }

  /**
   * Find all routes for a given telemetry message.
   *
   * <p>A message may need routed to multiple profiles should it be needed by more than one.  A
   * message may also not be routed should it not be needed by any profiles.
   *
   * @param json The raw JSON message.
   * @param profilerConfig The profile definitions.
   * @return A list of message routes.
   */
  private static List<MessageRoute> findRoutes(String json, ProfilerConfig profilerConfig) {
    List<MessageRoute> routes;
    Context context = getContext();

    // parse the raw message
    JSONParser parser = new JSONParser();
    Optional<JSONObject> message = toMessage(json, parser);
    if(message.isPresent()) {

      // find all routes
      MessageRouter router = new DefaultMessageRouter(context);
      routes = router.route(message.get(), profilerConfig, context);
      LOG.trace("Found {} route(s) for a message", routes.size());

    } else {
      // the message is not valid and must be ignored
      routes = Collections.emptyList();
    }

    return routes;
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

    } catch(ParseException e) {
      LOG.warn(String.format("Unable to parse message, message will be ignored; message='%s'", json), e);
      return Optional.empty();
    }
  }

  private static <T> Stream<T> toStream(Iterator<T> iterator) {
    Iterable<T> iterable = () -> iterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  /**
   * Produces a key to ensure that the telemetry is grouped by {profile, entity, period}.
   *
   * @param route The message route to generate the group key for.
   * @param periodDuration The period duration.
   * @param periodDurationUnits The units of the period duration.
   * @return The key to use when grouping.
   */
  private static String groupByKey(MessageRoute route, long periodDuration, TimeUnit periodDurationUnits) {
    ProfilePeriod period = ProfilePeriod.fromTimestamp(route.getTimestamp(), periodDuration, periodDurationUnits);
    return route.getProfileDefinition().getProfile() + "-" + route.getEntity() + "-" + period.getPeriod();
  }

  /**
   * Build a profile from a set of message routes.
   *
   * <p>This assumes that all of the necessary routes have been provided
   *
   * @param routesIter The message routes.
   * @param periodDurationMillis The period duration in milliseconds.
   * @return
   */
  private static ProfileMeasurementAdapter buildProfile(Iterator<MessageRoute> routesIter, long periodDurationMillis) {
    // create the distributor
    // some settings are unnecessary as the distributor is cleaned-up immediately after processing the batch
    int maxRoutes = Integer.MAX_VALUE;
    long profileTTLMillis = Long.MAX_VALUE;
    MessageDistributor distributor = new DefaultMessageDistributor(periodDurationMillis, profileTTLMillis, maxRoutes);
    Context context = getContext();

    // sort the messages/routes
    List<MessageRoute> routes = toStream(routesIter)
            .sorted(comparing(rt -> rt.getTimestamp()))
            .collect(Collectors.toList());
    LOG.debug("Building a profile from {} message(s)", routes.size());

    // apply each message/route to build the profile
    for(MessageRoute route: routes) {
      distributor.distribute(route, context);
    }

    // flush the profile
    List<ProfileMeasurement> measurements = distributor.flush();
    if(measurements.size() > 1) {
      throw new IllegalStateException("No more than 1 profile measurement is expected");
    }

    ProfileMeasurement m = measurements.get(0);
    LOG.debug("Profile measurement created; profile={}, entity={}, period={}",
            m.getProfileName(), m.getEntity(), m.getPeriod().getPeriod());
    return new ProfileMeasurementAdapter(m);
  }

  /**
   * Writes a set of measurements to HBase.
   *
   * @param iterator The measurements to write.
   * @param tableName The name of the HBase table.
   * @param columnFamily The HBase column family to write to.
   * @param periodDurationMillis The period duration in milliseconds.
   * @param saltDivisor The salt divisor.
   * @param durability The durability guarantee for HBase.
   * @return The number of measurements written to HBase.
   */
  private static Iterator<Integer> writeToHBase(Iterator<ProfileMeasurementAdapter> iterator,
                                  String tableName,
                                  String columnFamily,
                                  long periodDurationMillis,
                                  int saltDivisor,
                                  Durability durability) {
    LOG.debug("About to write profile measurement(s) to HBase");

    // open an HBase connection
    Configuration config = HBaseConfiguration.create();
    try(HBaseClient client = new HBaseClient(new HTableProvider(), config, tableName)) {

      RowKeyBuilder rowKeyBuilder = new SaltyRowKeyBuilder(saltDivisor, periodDurationMillis, TimeUnit.MILLISECONDS);
      ColumnBuilder columnBuilder = new ValueOnlyColumnBuilder(columnFamily);

      while(iterator.hasNext()) {
        ProfileMeasurement m = iterator.next().toProfileMeasurement();
        client.addMutation(rowKeyBuilder.rowKey(m), columnBuilder.columns(m), durability);
      }

      int count = client.mutate();
      LOG.debug("{} profile measurement(s) written to HBase", count);
      return IteratorUtils.singletonIterator(count);

    } catch(IOException e) {
      LOG.error("Unable to open connection to HBase", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * TODO replace this with something configurable by the user
   * TODO user should be able to get global config from Zookeeper
   */
  private static Context getContext() {
    Map<String, Object> global = new HashMap<>();
    Context context = new Context.Builder()
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
            .with(Context.Capabilities.STELLAR_CONFIG, () -> global)
            .build();
    StellarFunctions.initialize(context);
    return context;
  }
}