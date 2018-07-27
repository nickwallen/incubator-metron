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
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.IOUtils;
import org.apache.metron.common.configuration.profiler.ProfilerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.Properties;

import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.CONFIGURATION_FILE;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.GLOBALS_FILE;
import static org.apache.metron.profiler.spark.BatchProfilerCLIOptions.PROFILE_DEFN_FILE;
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
 *     -c profiler.properties \
 *     -g global.properties \
 *     -p profiles.json
 * }</pre>
 */
public class BatchProfilerCLI implements Serializable {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static void main(String[] args) throws IOException, org.apache.commons.cli.ParseException {
    // parse the command line
    CommandLine commandLine = parseCommandLine(args);
    Properties config = getConfiguration(commandLine);
    Properties globals = getGlobals(commandLine);
    ProfilerConfig profiles = getProfileDefinitions(commandLine);

    // the batch profiler must run in 'event time' mode
    if(!profiles.getTimestampField().isPresent()) {
      throw new IllegalArgumentException("The Batch Profiler must use event time. The 'timestampField' must be defined.");
    }

    BatchProfiler profiler = new BatchProfiler();
    long count = profiler.execute(config, globals, profiles);

    LOG.info("Profiler produced {} profile measurement(s)", count);
  }

  /**
   * Load the Stellar globals from a file.
   *
   * @param commandLine The command line.
   */
  private static Properties getGlobals(CommandLine commandLine) throws IOException {
    Properties globals = new Properties();
    if(GLOBALS_FILE.has(commandLine)) {
      String globalsPath = GLOBALS_FILE.get(commandLine);

      LOG.info("Loading global properties from '{}'", globalsPath);
      globals.load(new FileInputStream(globalsPath));

      LOG.info("Globals = {}", globals);
    }
    return globals;
  }

  /**
   * Load the Profiler configuration from a file.
   *
   * @param commandLine The command line.
   */
  private static Properties getConfiguration(CommandLine commandLine) throws IOException {
    Properties config = new Properties();
    if(CONFIGURATION_FILE.has(commandLine)) {
      String propertiesPath = CONFIGURATION_FILE.get(commandLine);

      LOG.info("Loading profiler configuration from '{}'", propertiesPath);
      config.load(new FileInputStream(propertiesPath));

      LOG.info("Properties = {}", config.toString());
    }
    return config;
  }

  /**
   * Load the profile definitions from a file.
   *
   * @param commandLine The command line.
   */
  private static ProfilerConfig getProfileDefinitions(CommandLine commandLine) throws IOException {
    ProfilerConfig profiles;
    if(PROFILE_DEFN_FILE.has(commandLine)) {
      String profilePath = PROFILE_DEFN_FILE.get(commandLine);

      LOG.info("Loading profiles from '{}'", profilePath);
      String contents = IOUtils.toString(new FileInputStream(profilePath));

      profiles = ProfilerConfig.fromJSON(contents);
      LOG.info("Loaded {} profile(s)", profiles.getProfiles().size());

    } else {
      throw new IllegalArgumentException("No profile(s) defined");
    }
    return profiles;
  }

  /**
   * Parse the command line arguments submitted by the user.
   * @param args The command line arguments to parse.
   * @throws org.apache.commons.cli.ParseException
   */
  private static CommandLine parseCommandLine(String[] args) throws ParseException {
    CommandLineParser parser = new PosixParser();
    return parse(parser, args);
  }
}