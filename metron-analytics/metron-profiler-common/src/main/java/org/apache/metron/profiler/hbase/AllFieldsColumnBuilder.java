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
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.profiler.ProfileMeasurement;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * A {@code ColumnBuilder} that stores all of the {@code ProfileMeasurement} fields to HBase.
 *
 * <p>This provides a much more detailed view into the measurements that are persisted in HBase.
 */
public class AllFieldsColumnBuilder implements ColumnBuilder {

  /**
   * The column family storing the profile data.
   */
  private String columnFamily;

  private byte[] columnFamilyBytes;

  /**
   * Defines all of the columns in HBase.
   */
  private static final Map<String, byte[]> columns;
  static {
    columns = new HashMap<>();
    columns.put("value", Bytes.toBytes("value"));
    columns.put("name", Bytes.toBytes("name"));
    columns.put("entity", Bytes.toBytes("entity"));
    columns.put("definition", Bytes.toBytes("definition"));
    columns.put("start", Bytes.toBytes("start"));
    columns.put("end", Bytes.toBytes("end"));
    columns.put("period", Bytes.toBytes("period"));
  }

  public AllFieldsColumnBuilder() {
    setColumnFamily("P");
  }

  public AllFieldsColumnBuilder(String columnFamily) {
    setColumnFamily(columnFamily);
  }

  @Override
  public ColumnList columns(ProfileMeasurement measurement) {
    ColumnList cols = new ColumnList();
    addColumn(cols, "value", measurement.getProfileValue());
    addColumn(cols, "name", measurement.getProfileName());
    addColumn(cols, "entity", measurement.getEntity());
    addColumn(cols, "definition", measurement.getDefinition());
    addColumn(cols, "start", measurement.getPeriod().getStartTimeMillis());
    addColumn(cols, "end", measurement.getPeriod().getEndTimeMillis());
    addColumn(cols, "period", measurement.getPeriod().getPeriod());
    return cols;
  }

  @Override
  public String getColumnFamily() {
    return this.columnFamily;
  }

  @Override
  public Collection<String> getColumns() {
    return new ArrayList<>(columns.keySet());
  }

  public void setColumnFamily(String columnFamily) {
    this.columnFamily = columnFamily;
    this.columnFamilyBytes = Bytes.toBytes(columnFamily);
  }

  public byte[] getColumnFamilyBytes() {
    return columnFamilyBytes;
  }

  @Override
  public byte[] getColumnQualifier(String fieldName) {
    if(columns.containsKey(fieldName)) {
      return columns.get(fieldName);

    } else {
      throw new IllegalArgumentException(("Unexpected field name: " + fieldName));
    }
  }

  private void addColumn(ColumnList cols, String name, Object value) {
    cols.addColumn(columnFamilyBytes, getColumnQualifier(name), SerDeUtils.toBytes(value));
  }

}
