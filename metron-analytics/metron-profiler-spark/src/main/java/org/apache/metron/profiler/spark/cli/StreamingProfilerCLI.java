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
package org.apache.metron.profiler.spark.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.apache.metron.profiler.spark.BatchProfiler;
import org.apache.metron.profiler.spark.StreamingProfiler;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.Properties;

import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.PERIOD_DURATION_UNITS;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.TELEMETRY_INPUT_FORMAT;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG;
import static org.apache.metron.profiler.spark.BatchProfilerConfig.WINDOW_LAG_UNITS;

public class StreamingProfilerCLI {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static Properties globals;
  public static Properties profilerProps;
  public static Properties readerProps;
  public static Properties writerProps;
  public static ProfilerConfig profiles;

  // TODO get rid of throws Exception
  public static void main(String[] args) throws Exception {

    LoggerFactory.getLogger("org.apache.spark");
    // parse the command line
//    CommandLine commandLine = parseCommandLine(args);
//    profilerProps = handleProfilerProperties(commandLine);
//    globals = handleGlobals(commandLine);
//    profiles = handleProfileDefinitions(commandLine);
//    readerProps = handleReaderProperties(commandLine);

    // TODO replace with CLI
    profilerProps = new Properties();
    profilerProps.put(TELEMETRY_INPUT_FORMAT.getKey(), "kafka");
    profilerProps.put(PERIOD_DURATION.getKey(), 30);
    profilerProps.put(PERIOD_DURATION_UNITS.getKey(), "seconds");
    profilerProps.put(WINDOW_LAG.getKey(), 5);
    profilerProps.put(WINDOW_LAG_UNITS.getKey(), "seconds");

    // TODO replace with CLI
    readerProps = new Properties();
    readerProps.put("subscribe", "test");
    readerProps.put("kafka.bootstrap.servers", "localhost:9092");
    readerProps.put("startingOffsets", "earliest");

    globals = new Properties();

    writerProps = new Properties();

    // TODO dummy hello-world profile
    ProfileConfig profile = new ProfileConfig()
            .withProfile("hello-world")
            .withForeach("ip_src_addr")
            .withInit("count", "0")
            .withUpdate("count", "count+1")
            .withResult("count");

    profiles = new ProfilerConfig()
            .withTimestampField(Optional.of("timestamp"))
            .withProfile(profile);

    // one or more profiles must be defined
    if(profiles.getProfiles().size() == 0) {
      throw new IllegalArgumentException("No profile definitions found.");
    }

    // TODO this should be defined by user CLI
    SparkConf conf = new SparkConf();
    conf.set("spark.master", "local[*]");

    SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

    spark.sparkContext().setLogLevel("ERROR");

    StreamingProfiler profiler = new StreamingProfiler();
    long count = profiler.run(spark, profilerProps, globals, readerProps, writerProps, profiles);
    LOG.info("Profiler produced {} profile measurement(s)", count);
  }
}
