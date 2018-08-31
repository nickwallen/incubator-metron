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

package org.apache.metron.pcap.config;

public class PcapGlobalDefaults {
  public static final String BASE_PCAP_PATH_DEFAULT = "/apps/metron/pcap";
  public static final String BASE_INPUT_PATH_DEFAULT = BASE_PCAP_PATH_DEFAULT + "/input";
  public static final String BASE_INTERIM_RESULT_PATH_DEFAULT = BASE_PCAP_PATH_DEFAULT + "/interim";
  public static final String FINAL_OUTPUT_PATH_DEFAULT = BASE_PCAP_PATH_DEFAULT + "/output";
  public static final int NUM_REDUCERS_DEFAULT = 10;
  public static final int NUM_RECORDS_PER_FILE_DEFAULT = 10000;
  public static final String NUM_FINALIZER_THREADS_DEFAULT = "1";
}
