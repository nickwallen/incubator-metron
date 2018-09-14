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
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.profiler.MessageRoute;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.spark.function.GroupByPeriodFunction;
import org.apache.metron.profiler.spark.function.HBaseWriterFunction;
import org.apache.metron.profiler.spark.function.JsonParserFunction;
import org.apache.metron.profiler.spark.function.MessageRouterFunction;
import org.apache.metron.profiler.spark.function.ProfileBuilderFunction;
import org.apache.metron.profiler.spark.function.TimestampExtractorFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_PATH;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG_UNITS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

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
  public long run(SparkSession spark,
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

    // TODO need to handle if profiler using processing time
    String timestampField = profiles.getTimestampField().get();
    int periodDuration = PERIOD_DURATION.get(profilerProps, Integer.class);
    String periodDurationUnits = PERIOD_DURATION_UNITS.get(profilerProps, String.class);

    String windowLag = String.join(" ",
            WINDOW_LAG.get(profilerProps, Integer.class).toString(),
            WINDOW_LAG_UNITS.get(profilerProps, String.class).toLowerCase());

    //    profilerProperties.put("profiler.window.lag", Long.toString(windowLagMillis));
//    profilerProperties.put("profiler.window.lag.units", "MILLISECONDS");

    // fetch the streaming telemetry
    Dataset<String> telemetry = spark
            .readStream()
            .format(inputFormat)
            .options(Maps.fromProperties(readerProps))
            .load()
            .select("value")
            .as(Encoders.STRING());

    telemetry.map(new TimestampExtractorFunction(timestampField), Encoders.bean(JSONObject.class), Encoders.TIMESTAMP());

    Dataset<MessageRoute> routes = telemetry
            .flatMap(new MessageRouterFunction(profiles, globals), Encoders.bean(MessageRoute.class));

//    Dataset<Row> foo = telemetry.select("value").map(row -> parseJSON(row.getString(0)))
//            .map(row -> {
//      JSONParser parser = new JSONParser();
//      try {
//        JSONObject message = (JSONObject) parser.parse(row.));
//        return message;
//
//      } catch(Throwable e) {
//        LOG.warn(String.format("Unable to parse message, message will be ignored"), e);
//        return null;
//      }
//    }, Encoders.STRING());

    // TEMP to view results of above
    StreamingQuery query = routes
            .writeStream()
            .trigger(Trigger.Once())
            .format("console")
            .start();
    query.awaitTermination();
//
//            .selectExpr("CAST(value AS STRING)")
//            .as(Encoders.STRING());


    // TODO where do we group over a period of time, then flush???  Right now everything in one trigger is flushed together?
    // TODO instead of relying on groupByKey after we get the routes, do we have to do that here?
    // TODO where do we group over a period of time, then flush???  Right now everything in one trigger is flushed together?
    // TODO or does this already work

    // find all routes for each message
//    Dataset<MessageRoute> routes = telemetry
//            .flatMap(new MessageRouterFunction(profiles, globals), Encoders.bean(MessageRoute.class));

    // TODO need to set the window size and lag based on profiler settings

    // build the profiles
//    Dataset<ProfileMeasurementAdapter> measurements = routes
//            .withWatermark(timestampField, windowLag)
//            .groupByKey(new GroupByPeriodFunction(profilerProps), Encoders.STRING())
//            .mapGroups(new ProfileBuilderFunction(profilerProps, globals), Encoders.bean(ProfileMeasurementAdapter.class));

    // TODO TELEMETRY_OUTPUT_FORMAT; outputFormat = "kafka"
    String outputFormat = "console";

    // write the profile measurements
//    StreamingQuery profiler = measurements
//            .writeStream()
//            .trigger(Trigger.Once())
//            .options(Maps.fromProperties(writerProps))
//            .format(outputFormat)
//            .outputMode("complete")
//            .start();

//    profiler.awaitTermination();

    // TODO
    int count = 0;
    return count;
  }
}
