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

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.profiler.ProfileMeasurement;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Tests the ValueOnlyColumnBuilder class.
 */
public class ValueOnlyColumnBuilderTest {

  @Test
  public void testBuildColumns() {

    ProfileMeasurement measurement = new ProfileMeasurement()
            .withProfileName("profile")
            .withEntity("entity")
            .withProfileValue(234);

    ValueOnlyColumnBuilder builder = new ValueOnlyColumnBuilder();
    ColumnList columnList = builder.columns(measurement);

    // expect to see a list of columns
    assertNotNull(columnList);
    assertTrue(columnList.hasColumns());

    // expect only 1 column
    List<ColumnList.Column> columns = columnList.getColumns();
    assertEquals(1, columns.size());

    // expect the column to be for 'value' only
    for(ColumnList.Column col: columns) {
      byte[] expected = builder.getColumnQualifier("value");
      assertArrayEquals(expected, col.getQualifier());
    }
  }

  @Test
  public void testColumnQualifierForValue() {

    ValueOnlyColumnBuilder builder = new ValueOnlyColumnBuilder();

    // expect only 1 column qualifier
    byte[] expected = Bytes.toBytes("value");
    assertArrayEquals(expected, builder.getColumnQualifier("value"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnQualifierThatDoesNotExist() {

    ValueOnlyColumnBuilder builder = new ValueOnlyColumnBuilder();

    // expect no column qualifier by this name
    builder.getColumnQualifier("does-not-exist");
  }
}
