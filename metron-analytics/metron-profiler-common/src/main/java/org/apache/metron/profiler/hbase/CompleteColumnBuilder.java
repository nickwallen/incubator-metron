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

import java.util.Arrays;
import java.util.List;

/**
 * A ColumnBuilder that writes the complete profile measurement to HBase.  This includes a column each for the
 * value, profile name, entity, window duration, and period.
 */
public class CompleteColumnBuilder implements ColumnBuilder {

  private static final List<String> COLUMNS = Arrays.asList("value", "profile", "entity", "window", "period");

  /**
   * The column family storing the profile data.
   */
  private String columnFamily;

  private byte[] columnFamilyBytes;

  public CompleteColumnBuilder() {
    setColumnFamily("P");
  }

  public CompleteColumnBuilder(String columnFamily) {
    setColumnFamily(columnFamily);
  }

  @Override
  public ColumnList columns(ProfileMeasurement measurement) {
    ColumnList cols = new ColumnList();
    cols.addColumn(columnFamilyBytes, getColumnQualifier("value"), SerDeUtils.toBytes(measurement.getProfileValue()));
    cols.addColumn(columnFamilyBytes, getColumnQualifier("profile"), SerDeUtils.toBytes(measurement.getProfileName()));
    cols.addColumn(columnFamilyBytes, getColumnQualifier("entity"), SerDeUtils.toBytes(measurement.getEntity()));
    cols.addColumn(columnFamilyBytes, getColumnQualifier("window"), SerDeUtils.toBytes(measurement.getPeriod().getDurationMillis()));
    cols.addColumn(columnFamilyBytes, getColumnQualifier("period"), SerDeUtils.toBytes(measurement.getPeriod().getPeriod()));
    return cols;
  }

  @Override
  public String getColumnFamily() {
    return this.columnFamily;
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
    this.columnFamilyBytes = Bytes.toBytes(columnFamily);
  }

  @Override
  public byte[] getColumnQualifier(String fieldName) {
    if(COLUMNS.contains(fieldName)) {
      return Bytes.toBytes(fieldName);
    }

    throw new IllegalArgumentException(("unexpected field name: " + fieldName));
  }
}