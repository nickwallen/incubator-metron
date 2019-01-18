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

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.Maps;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.profiler.MessageRoute;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.spark.function.GroupByPeriodFunction;
import org.apache.metron.profiler.spark.function.HBaseWriterFunction;
import org.apache.metron.profiler.spark.function.JsonParserFunction;
import org.apache.metron.profiler.spark.function.MessageRouterFunction;
import org.apache.metron.profiler.spark.function.ProfileBuilderFunction;
import org.apache.metron.profiler.spark.function.TimestampExtractorFunction;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.lang.invoke.MethodHandles;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_PATH;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG_UNITS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.json_tuple;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.window;

public class StreamingProfiler {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * Execute the Streaming Profiler in Spark.
   *
   * @param spark The spark session.
   * @param profilerProps The profiler configuration properties.
   * @param globalProps The Stellar global properties.
   * @param readerProps The properties passed to the streaming {@link org.apache.spark.sql.DataFrameReader}.
   * @param writerProps The properties passed to the streaming writer
   * @param profiles The profile definitions.
   * @return The number of profile measurements produced.
   */
  public long run(SparkConf sparkConf,
                  Properties profilerProps,
                  Properties globalProps,
                  Properties readerProps,
                  Properties writerProps,
                  ProfilerConfig profiles) throws Exception {

    LOG.debug("Building {} profile(s)", profiles.getProfiles().size());
    Map<String, String> globals = Maps.fromProperties(globalProps);

    // TODO the profile definitions should come from Zk

    String inputFormat = TELEMETRY_INPUT_FORMAT.get(profilerProps, String.class);
    String inputPath = TELEMETRY_INPUT_PATH.get(profilerProps, String.class);
    LOG.debug("inputFormat = {}", inputFormat);

    // TODO need to handle if profiler using processing time
    String timestampField = profiles.getTimestampField().get();

    long periodDurationMillis = TimeUnit.valueOf(PERIOD_DURATION_UNITS.get(profilerProps, String.class))
            .toMillis(PERIOD_DURATION.get(profilerProps, Integer.class));
    LOG.debug("periodDuration = {}", periodDurationMillis);

    String windowLag = String.join(" ",
            WINDOW_LAG.get(profilerProps, Integer.class).toString(),
            WINDOW_LAG_UNITS.get(profilerProps, String.class).toLowerCase());
    LOG.debug("windowLag = {}", windowLag);


    List<String> topics = Collections.singletonList("test");

    Map<String, Object> readerConf = (Map) readerProps;
    JavaStreamingContext context = new JavaStreamingContext(sparkConf, new Duration(1000));

    // TODO remove this
    context.sparkContext().setLogLevel("ERROR");

    JavaInputDStream<ConsumerRecord<String, String>> telemetry = KafkaUtils.createDirectStream(
            context,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, readerConf));

    // find all routes for each message
    JavaDStream<MessageRoute> routes = telemetry
            .map(record -> record.value())
            .flatMap(new MessageRouterFunction(profiles, globals));

    // TODO need to set the window size and lag based on profiler settings
    routes
            .mapToPair(route -> new Tuple2<String, MessageRoute>(route.getEntity(), route))
            .groupByKeyAndWindow(new Duration(periodDurationMillis))
            .map((pair) -> new ProfileBuilderFunction(profilerProps, globals).call(pair._1(), pair._2().iterator()))
            .print(20);

    // build the profiles
//    JavaDStream<ProfileMeasurementAdapter> measurements = routes
//            .withWatermark(timestampField, windowLag)
//            .groupByKey(new GroupByPeriodFunction(profilerProps), Encoders.STRING())
//            .mapGroups(new ProfileBuilderFunction(profilerProps, globals), Encoders.bean(ProfileMeasurementAdapter.class));

    context.start();
    context.awaitTermination();

    // TODO
    int count = 0;
    return count;
  }

}
