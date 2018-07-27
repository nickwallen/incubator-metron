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
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Optional;
import java.util.Properties;

import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.GLOBALS_FILE;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.PROPERTIES_FILE;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.parse;

/**
 * The main entry point which launches the Batch Profiler in Spark.
 *
 * The Batch Profiler can be submitted using the following command.
 * <pre>{@code
 *  $SPARK_HOME/bin/spark-submit \
 *    --class org.apache.metron.profiler.spark.BatchProfilerCLI \
 *     --properties-file profiler.properties \
 *     metron-profiler-spark-<version>.jar \
 *     -p profiler.properties
 * }</pre>
 */
public class BatchProfilerCLI implements Serializable {

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

  private static CommandLine parseCommandLine(String[] args) throws org.apache.commons.cli.ParseException {
    CommandLineParser parser = new PosixParser();
    return parse(parser, args);
  }

  public static void main(String[] args) throws IOException, org.apache.commons.cli.ParseException {
    CommandLine commandLine = parseCommandLine(args);

    // load the profiler properties from a file, if one exists
    Properties properties = new Properties();
    if(PROPERTIES_FILE.has(commandLine)) {
      String propertiesPath = PROPERTIES_FILE.get(commandLine);

      LOG.info("Loading profiler properties from '{}'", propertiesPath);
      properties.load(new FileInputStream(propertiesPath));

      LOG.info("Properties = {}", properties.toString());
    }

    // load the global properties for Stellar execution from a file, if one exists
    Properties globals = new Properties();
    if(GLOBALS_FILE.has(commandLine)) {
      String globalsPath = GLOBALS_FILE.get(commandLine);

      LOG.info("Loading global properties from '{}'", globalsPath);
      globals.load(new FileInputStream(globalsPath));

      LOG.info("Globals = {}", globals);
    }

    BatchProfiler profiler = new BatchProfiler();
    long count = profiler.execute(properties, globals, getProfilerConfig());

    LOG.info("Profiler produced {} profile measurement(s)", count);
  }


}