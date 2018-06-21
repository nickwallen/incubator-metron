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
import org.apache.metron.common.utils.SerDeUtils;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.hbase.bolt.mapper.ColumnList;

import java.util.Collection;
import java.util.Collections;

/**
 * A {@link ColumnBuilder} that stores only the value of a {@link ProfileMeasurement} in HBase.
 */
public class ValueOnlyColumnBuilder implements ColumnBuilder {

  public static final String VALUE_FIELD = "value";
  public static final byte[] VALUE_FIELD_COLUMN_QUALIFIER = Bytes.toBytes(VALUE_FIELD);

  /**
   * The column family storing the profile data.
   */
  private String columnFamily;

  private byte[] columnFamilyBytes;

  public ValueOnlyColumnBuilder() {
    setColumnFamily("P");
  }

  public ValueOnlyColumnBuilder(String columnFamily) {
    setColumnFamily(columnFamily);
  }

  @Override
  public ColumnList columns(ProfileMeasurement measurement) {
    ColumnList cols = new ColumnList();
    cols.addColumn(columnFamilyBytes, VALUE_FIELD_COLUMN_QUALIFIER, SerDeUtils.toBytes(measurement.getProfileValue()));

    return cols;
  }

  @Override
  public String getColumnFamily() {
    return this.columnFamily;
  }

  @Override
  public Collection<String> getColumns() {
    return Collections.singleton(VALUE_FIELD);
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
    this.columnFamilyBytes = Bytes.toBytes(columnFamily);
  }

  @Override
  public byte[] getColumnQualifier(String fieldName) {
    if(VALUE_FIELD.equals(fieldName)) {
      return VALUE_FIELD_COLUMN_QUALIFIER;
    }

    throw new IllegalArgumentException(("unexpected field name: " + fieldName));
  }
}