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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
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
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Comparator.comparing;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.PROPERTIES_FILE;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.parse;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_COLUMN_FAMILY;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_SALT_DIVISOR;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.HBASE_TABLE_NAME;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.MAX_ROUTES;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PROFILE_TTL;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PROFILE_TTL_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_PATH;

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

  public static ProfileMeasurementAdapter buildProfile(Iterator<MessageRouteAdapter> routes,
                                                       long periodDurationMillis,
                                                       long profileTTLMillis,
                                                       int maxRoutes) {
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

  public static void writeToHBase(Iterator<ProfileMeasurementAdapter> iterator,
                                  String tableName,
                                  String columnFamily,
                                  long periodDurationMillis,
                                  int saltDivisor,
                                  Durability durability) {
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

  private static CommandLine parseCommandLine(String[] args) throws org.apache.commons.cli.ParseException {
    /*
     * The general gist is that in order to pass additional args to Spark,
     * we have to disregard options that we don't know about in the CLI.
     * Storm will ignore our args, we have to do the same.
     */
    CommandLineParser parser = new PosixParser() {
      @Override
      protected void processOption(String arg, ListIterator iter) throws org.apache.commons.cli.ParseException {

        // TODO is this needed?  do we need to allow user to pass additional options to spark or anything?
        if(getOptions().hasOption(arg)) {
          super.processOption(arg, iter);
        }
      }
    };
    return parse(parser, args);
  }

  public static void main(String[] args) throws IOException, org.apache.commons.cli.ParseException {
    CommandLine commandLine = parseCommandLine(args);

    // load config from a properties file, if one is defined
    Properties config = new Properties();
    if(PROPERTIES_FILE.has(commandLine)) {
      String propertiesPath = PROPERTIES_FILE.get(commandLine);

      LOG.debug("Loading profiler properties from '{}'", propertiesPath);
      config.load(new FileInputStream(propertiesPath));
    }

    // required configuration values
    TimeUnit periodDurationUnits = TimeUnit.valueOf(PERIOD_DURATION_UNITS.get(config, String.class));
    int periodDuration = PERIOD_DURATION.get(config, Integer.class);
    long periodDurationMillis = periodDurationUnits.toMillis(periodDuration);
    TimeUnit profileTTLUnits = TimeUnit.valueOf(PROFILE_TTL_UNITS.get(config, String.class));
    int profileTTL = PROFILE_TTL.get(config, Integer.class);
    long profileTTLMillis = profileTTLUnits.toMillis(profileTTL);
    int maxRoutes = MAX_ROUTES.get(config, Integer.class);
    int saltDivisor = HBASE_SALT_DIVISOR.get(config, Integer.class);
    String tableName = HBASE_TABLE_NAME.get(config, String.class);
    String columnFamily = HBASE_COLUMN_FAMILY.get(config, String.class);
    String inputFormat = TELEMETRY_INPUT_FORMAT.get(config, String.class);
    String inputPath = TELEMETRY_INPUT_PATH.get(config, String.class);

    SparkSession spark = SparkSession
            .builder()
            .config(getConfig())
            .master("local") // TODO remove this
            .getOrCreate();

    ProfilerConfig profilerConfig = getProfilerConfig();
    LOG.debug("Building {} profile(s)", profilerConfig.getProfiles().size());

    // fetch the archived telemetry
    LOG.debug("Loading telemetry from '{}'", inputPath);
    Dataset<String> telemetry = spark
            .read()
            .format(inputFormat)
            .load(inputPath)
            .as(Encoders.STRING());
    LOG.debug("Found {} telemetry record(s)", telemetry.count());

    // find all routes for each message
    FlatMapFunction<String, MessageRouteAdapter> findRoutesFn = msg -> findRoutes(msg, profilerConfig).iterator();
    Dataset<MessageRouteAdapter> allRoutes = telemetry.flatMap(findRoutesFn, Encoders.bean(MessageRouteAdapter.class));
    LOG.debug("Generated {} message route(s)", allRoutes.count());

    // build the profiles
    MapFunction<MessageRouteAdapter, String> groupByPeriodFn =
            route -> groupByPeriod(route, periodDuration, periodDurationUnits);
    MapGroupsFunction<String, MessageRouteAdapter, ProfileMeasurementAdapter> buildProfilesFn =
            (grp, routes) -> buildProfile(routes, periodDurationMillis, profileTTLMillis, maxRoutes);
    Dataset<ProfileMeasurementAdapter> measurements = allRoutes
            .groupByKey(groupByPeriodFn, Encoders.STRING())
            .mapGroups(buildProfilesFn, Encoders.bean(ProfileMeasurementAdapter.class));
    LOG.debug("Produced {} profile measurement(s)", measurements.count());

    // write the values to HBase
    ForeachPartitionFunction<ProfileMeasurementAdapter> writeToHBaseFn = iter ->
            writeToHBase(iter, tableName, columnFamily, periodDurationMillis, saltDivisor, Durability.USE_DEFAULT);
    measurements.foreachPartition(writeToHBaseFn);
    LOG.debug("All profile measurement(s) written to HBase");
  }


}