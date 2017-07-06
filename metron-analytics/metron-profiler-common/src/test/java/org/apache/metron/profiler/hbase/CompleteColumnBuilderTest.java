package org.apache.metron.profiler.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.bolt.mapper.ColumnList;
import org.apache.metron.profiler.ProfileMeasurement;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the CompleteColumnBuilderTest.
 */
public class CompleteColumnBuilderTest {

  private CompleteColumnBuilder builder;

  @Before
  public void setup() {
    builder = new CompleteColumnBuilder();
  }

  @Test
  public void testBuildColumns() {

    ProfileMeasurement measurement = new ProfileMeasurement()
            .withProfileName("profile")
            .withEntity("entity")
            .withPeriod(System.currentTimeMillis(), 15, TimeUnit.MINUTES)
            .withProfileValue(234);

    ColumnList columnList = builder.columns(measurement);

    // expect to see a list of columns
    assertNotNull(columnList);
    assertTrue(columnList.hasColumns());

    // expect to see 5 columns
    List<ColumnList.Column> columns = columnList.getColumns();
    assertEquals(5, columns.size());

    // expect the column qualifiers to be one of the following
    List<byte[]> expected = new ArrayList<>();
    expected.add(builder.getColumnQualifier("value"));
    expected.add(builder.getColumnQualifier("profile"));
    expected.add(builder.getColumnQualifier("entity"));
    expected.add(builder.getColumnQualifier("window"));
    expected.add(builder.getColumnQualifier("period"));

    for(ColumnList.Column column: columns) {

      boolean found = false;
      for(byte[] e : expected) {
        if(Arrays.equals(e, column.getQualifier())) {
          found = true;
        }
      }

      assertTrue(found);
    }
  }

  @Test
  public void testColumnQualifiers() {

    assertArrayEquals(Bytes.toBytes("value"), builder.getColumnQualifier("value"));
    assertArrayEquals(Bytes.toBytes("profile"), builder.getColumnQualifier("profile"));
    assertArrayEquals(Bytes.toBytes("entity"), builder.getColumnQualifier("entity"));
    assertArrayEquals(Bytes.toBytes("window"), builder.getColumnQualifier("window"));
    assertArrayEquals(Bytes.toBytes("period"), builder.getColumnQualifier("period"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testColumnQualifierThatDoesNotExist() {

    // expect no column qualifier by this name
    builder.getColumnQualifier("does-not-exist");
  }
}
