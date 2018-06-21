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
package org.apache.metron.profiler.hbase;

import org.apache.metron.profiler.ProfileMeasurement;

import java.util.function.Function;

/**
 * Enumerates a set of {@link ColumnBuilder} implementations.
 *
 * <p>Provides shared instances of each {@link ColumnBuilder}.
 *
 * <p>Allows the field name converter to be specified using a short-hand
 * name, rather than the entire fully-qualified class name.
 */
public enum ColumnBuilders {

  /**
   * A {@link ColumnBuilder} that stores only the value of a {@link ProfileMeasurement} in Hbase.
   */
  VALUES_ONLY((arg) -> new ValueOnlyColumnBuilder()),

  /**
   * A {@link ColumnBuilder} that stores only the value of a {@link ProfileMeasurement} in Hbase.
   *
   * <p>Allows a custom column family to be defined.
   */
  VALUES_ONLY_WITH_CF((columnFamily) -> new ValueOnlyColumnBuilder(columnFamily)),

  /**
   * A {@link ColumnBuilder} that stores all fields of a {@link ProfileMeasurement} in Hbase.
   */
  ALL_FIELDS((arg) -> new AllFieldsColumnBuilder()),

  /**
   * A {@link ColumnBuilder} that stores all fields of a {@link ProfileMeasurement} in Hbase.
   *
   * <p>Allows a custom column family to be defined.
   */
  ALL_FIELDS_WITH_CF((columnFamily) -> new AllFieldsColumnBuilder(columnFamily));

  /**
   * Responsible for creating the {@link ColumnBuilder} when its needed.
   */
  private Function<String, ColumnBuilder> createFn;

  ColumnBuilders(Function<String, ColumnBuilder> createFn) {
    this.createFn = createFn;
  }

  public ColumnBuilder get() {
    // the arg is not needed in this case
    String arg = null;
    return get(arg);
  }

  public ColumnBuilder get(String arg) {
    return createFn.apply(arg);
  }
}
