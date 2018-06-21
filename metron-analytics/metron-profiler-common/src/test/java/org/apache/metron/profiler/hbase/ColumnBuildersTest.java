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

import org.junit.Test;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests the {@link ColumnBuilders} class.
 */
public class ColumnBuildersTest {

  @Test
  public void testValuesOnly() {
    ColumnBuilder actual = ColumnBuilders.valueOf("VALUES_ONLY").get();
    assertThat(actual, instanceOf(ValueOnlyColumnBuilder.class));
    assertThat(actual.getColumnFamily(), equalTo("P"));
  }

  /**
   * Using VALUES_ONLY should still work, even if an arg (column family) is provided.
   */
  @Test
  public void testValuesOnlyWithArg() {
    ColumnBuilder actual = ColumnBuilders.valueOf("VALUES_ONLY").get("X");
    assertThat(actual, instanceOf(ValueOnlyColumnBuilder.class));
    assertThat(actual.getColumnFamily(), equalTo("P"));
  }

  @Test
  public void testValuesOnlyWithColumnFamily() {
    final String colFamily = "X";
    ColumnBuilder actual = ColumnBuilders.valueOf("VALUES_ONLY_WITH_CF").get(colFamily);
    assertThat(actual, instanceOf(ValueOnlyColumnBuilder.class));
    assertThat(actual.getColumnFamily(), equalTo(colFamily));
  }

  /**
   * Using VALUES_ONLY_WITH_CF, requires that the column family be provided. If none
   * is provided, an exception should be thrown.
   */
  @Test(expected = NullPointerException.class)
  public void testValuesOnlyWithColumnFamilyButNoneProvided() {
    // calling get() sets the column family to null, which is not allowed
    ColumnBuilders.valueOf("VALUES_ONLY_WITH_CF").get();
  }

  @Test
  public void testAllFields() {
    ColumnBuilder actual = ColumnBuilders.valueOf("ALL_FIELDS").get();
    assertThat(actual, instanceOf(AllFieldsColumnBuilder.class));
    assertThat(actual.getColumnFamily(), equalTo("P"));
  }

  @Test
  public void testAllFieldsWithColumnFamily() {
    final String colFamily = "X";
    ColumnBuilder actual = ColumnBuilders.valueOf("ALL_FIELDS_WITH_CF").get(colFamily);
    assertThat(actual, instanceOf(AllFieldsColumnBuilder.class));
    assertThat(actual.getColumnFamily(), equalTo(colFamily));
  }

  /**
   * Using ALL_FIELDS_WITH_CF, requires that the column family be provided. If none
   * is provided, an exception should be thrown.
   */
  @Test(expected = NullPointerException.class)
  public void testAllFieldsWithColumnFamilyButNoneProvided() {
    // calling get() sets the column family to null, which is not allowed
    ColumnBuilders.valueOf("ALL_FIELDS_WITH_CF").get();
  }
}
