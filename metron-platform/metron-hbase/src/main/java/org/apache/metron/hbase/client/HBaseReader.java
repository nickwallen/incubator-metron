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
package org.apache.metron.hbase.client;

import org.apache.hadoop.hbase.client.Result;
import org.apache.metron.hbase.bolt.mapper.HBaseProjectionCriteria;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * An {@link HBaseReader} is used to read data from HBase.
 */
public interface HBaseReader extends Closeable {

  /**
   * Enqueues a 'get' request that will be submitted when {@link #getAll()} is called.
   * @param rowKey The row key to be retrieved.
   */
  void addGet(byte[] rowKey, HBaseProjectionCriteria criteria);

  /**
   * Submits all pending get operations and returns the result of each.
   * @return The result of each pending get request.
   */
  Result[] getAll();

  /**
   * Clears all pending get operations.
   */
  void clearGets();

  /**
   * Scans an entire table returning all row keys as a List of Strings.
   *
   * <p><b>**WARNING**:</b> Do not use this method unless you're absolutely crystal clear about the performance
   * impact. Doing full table scans in HBase can adversely impact performance.
   *
   * @return List of all row keys as Strings for this table.
   */
  List<String> scanRowKeys() throws IOException;
}
