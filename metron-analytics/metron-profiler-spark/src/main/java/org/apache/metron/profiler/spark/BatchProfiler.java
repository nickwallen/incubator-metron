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
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.hbase.HTableProvider;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
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
import org.apache.spark.api.java.function.ForeachPartitionFunction;
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
import java.io.InputStream;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;

/**
 * The main program which launches the Batch Profiler in Spark.
 */
public class BatchProfiler {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * TODO replace this stub with something sensible
   * TODO allow user to either provide profile definitions in file or in zookeeper
   */
  public static ProfilerConfig getProfilerConfig() {

    ProfileConfig counterOne = new ProfileConfig()
            .withProfile("counter-1")
            .withForeach("ip_src_addr")
            .withInit("count", "0")
            .withUpdate("count", "count + 1")
            .withResult("count");

    ProfileConfig counterTwo = new ProfileConfig()
            .withProfile("counter-2")
            .withForeach("ip_src_addr")
            .withInit("count", "0")
            .withUpdate("count", "count + 1")
            .withResult("count");

    return new ProfilerConfig()
            .withTimestampField(Optional.of("timestamp"))
            .withProfile(counterOne)
            .withProfile(counterTwo);
  }

  /**
   * TODO replace this with something configurable by the user
   * TODO user should be able to get global config from Zookeeper
   */
  public static Context getContext() {
    Map<String, Object> global = new HashMap<>();
    Context context = new Context.Builder()
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
            .with(Context.Capabilities.STELLAR_CONFIG, () -> global)
            .build();
    StellarFunctions.initialize(context);
    return context;
  }

  private static JSONParser parser = new JSONParser();
  public static Optional<JSONObject> toMessage(String json) {
    try {
      JSONObject message = (JSONObject) parser.parse(json);
      return Optional.of(message);

    } catch(ParseException e) {
      return Optional.empty();
    }
  }

  public static List<MessageRouteAdapter> findRoutes(String json, ProfilerConfig profilerConfig) {
    List<MessageRouteAdapter> routes;

    Optional<JSONObject> message = toMessage(json);
    if(message.isPresent()) {
      // find all routes
      Context context = getContext();
      MessageRouter router = new DefaultMessageRouter(context);
      List<MessageRoute> originalRoutes = router.route(message.get(), profilerConfig, context);

      // return the adapter so they can be serialized by Spark more simply
      routes = originalRoutes
              .stream()
              .map(route -> new MessageRouteAdapter(route))
              .collect(Collectors.toList());

    } else {
      // unable to parse the json
      routes = Collections.emptyList();
    }

    return routes;
  }

  public static SparkConf getConfig() {
    SparkConf conf = new SparkConf();
    conf.registerKryoClasses(new Class[] { JSONObject.class });
    return conf;
  }

  public static <T> Stream<T> toStream(Iterator<T> iterator) {
    Iterable<T> iterable = () -> iterator;
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  public static String groupByPeriod(MessageRouteAdapter route, long periodDuration, TimeUnit periodDurationUnits) {
    ProfilePeriod period = ProfilePeriod.fromTimestamp(route.getTimestamp(), periodDuration, periodDurationUnits);
    // TODO more efficient way to serialize this?
    return route.getProfileName() + "-" + route.getEntity() + "-" + period.getPeriod();
  }

  public static ProfileMeasurementAdapter buildProfile(Iterator<MessageRouteAdapter> routes, long periodDurationMillis, long profileTTLMillis, int maxRoutes) {
    MessageDistributor distributor = new DefaultMessageDistributor(periodDurationMillis, profileTTLMillis, maxRoutes);
    Context context = getContext();

    // apply each message to the profile in timestamp order
    toStream(routes)
            .sorted(comparing(rt -> rt.getTimestamp()))
            .map(adapter -> adapter.toMessageRoute())
            .forEach(rt -> distributor.distribute(rt, context));

    // flush the profile
    List<ProfileMeasurement> measurements = distributor.flush();
    if(measurements.size() > 1) {
      throw new IllegalStateException("No more than 1 profile measurement is expected");
    }

    // return the adapter
    return new ProfileMeasurementAdapter(measurements.get(0));
  }

  public static void writeToHBase(Iterator<ProfileMeasurementAdapter> iterator, String tableName, String columnFamily, long periodDurationMillis, int saltDivisor, Durability durability) {

    // setup hbase client
    Configuration config = HBaseConfiguration.create();
    config.set("hbase.master.hostname", "localhost");
    config.set("hbase.regionserver.hostname", "localhost");

    // open an HBase connection
    try(HBaseClient client = new HBaseClient(new HTableProvider(), config, tableName)) {
      RowKeyBuilder rowKeyBuilder = new SaltyRowKeyBuilder(saltDivisor, periodDurationMillis, TimeUnit.MILLISECONDS);
      ColumnBuilder columnBuilder = new ValueOnlyColumnBuilder(columnFamily);

      while(iterator.hasNext()) {
        ProfileMeasurement m = iterator.next().toProfileMeasurement();
        client.addMutation(rowKeyBuilder.rowKey(m), columnBuilder.columns(m), durability);
      }

      client.mutate();

    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws IOException {

    SparkSession spark = SparkSession
            .builder()
            .config(getConfig())
            .master("local") // TODO remove this
            .getOrCreate();

    // TODO remove me
    spark.sparkContext().setLogLevel("WARN");

    // TODO allow the user to configure these
    String pathToMessages = "/Users/nallen/tmp/indexed/bro/";
    String inputFormat = "text";
    ProfilerConfig profilerConfig = getProfilerConfig();
    long periodDuration = 15;
    TimeUnit periodDurationUnits = TimeUnit.MINUTES;
    long profileTTL = 30;
    TimeUnit profileTTLUnits = TimeUnit.MINUTES;
    int saltDivisor = 1000;
    String tableName = "profiler";
    String columnFamily = "P";
    long periodDurationMillis = periodDurationUnits.toMillis(periodDuration);
    long profileTTLMillis = profileTTLUnits.toMillis(profileTTL);
    int maxRoutes = 100000;

    // fetch the archived telemetry
    Dataset<String> telemetry = spark
            .read()
            .format(inputFormat)
            .load(pathToMessages)
            .as(Encoders.STRING());

    // TODO remove me
    System.out.println("Telemetry Record(s): " + telemetry.count());
    telemetry.printSchema();

    // find all routes for each message
    FlatMapFunction<String, MessageRouteAdapter> findRoutes = msg -> findRoutes(msg, profilerConfig).iterator();
    Dataset<MessageRouteAdapter> allRoutes = telemetry.flatMap(findRoutes, Encoders.bean(MessageRouteAdapter.class));

    // TODO remove me
    System.out.println("Message Route(s): " + allRoutes.count());
    allRoutes.printSchema();

    // build the profiles
    MapFunction<MessageRouteAdapter, String> groupByPeriodFn =
            route -> groupByPeriod(route, periodDuration, periodDurationUnits);
    MapGroupsFunction<String, MessageRouteAdapter, ProfileMeasurementAdapter> buildProfilesFn =
            (grp, routes) -> buildProfile(routes, periodDurationMillis, profileTTLMillis, maxRoutes);

    Dataset<ProfileMeasurementAdapter> measurements = allRoutes
            .groupByKey(groupByPeriodFn, Encoders.STRING())
            .mapGroups(buildProfilesFn, Encoders.bean(ProfileMeasurementAdapter.class));

    // TODO remove me
    System.out.println("Measurement(s): " + measurements.count());
    measurements.printSchema();
//    measurements.explain(true);

    // write the values to HBase
    ForeachPartitionFunction<ProfileMeasurementAdapter> writeToHBaseFn = iter ->
            writeToHBase(iter, tableName, columnFamily, periodDurationMillis, saltDivisor, Durability.USE_DEFAULT);
    measurements.foreachPartition(writeToHBaseFn);
  }
}