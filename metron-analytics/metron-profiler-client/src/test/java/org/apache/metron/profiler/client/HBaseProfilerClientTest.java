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

package org.apache.metron.profiler.client;

import org.apache.metron.hbase.mock.MockHTable;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.ProfilePeriod;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.apache.metron.profiler.hbase.RowKeyBuilder;
import org.apache.metron.profiler.hbase.SaltyRowKeyBuilder;
import org.apache.metron.profiler.hbase.ValueOnlyColumnBuilder;
import org.apache.metron.stellar.common.DefaultStellarStatefulExecutor;
import org.apache.metron.stellar.common.StellarStatefulExecutor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.apache.metron.profiler.client.TestUtility.copy;
import static org.junit.Assert.assertEquals;

/**
 * Tests the HBaseProfilerClient.
 *
 * The naming used in this test attempts to be as similar to how the 'groupBy'
 * functionality might be used 'in the wild'.  This test involves reading and
 * writing two separate groups originating from the same Profile and Entity.
 * There is a 'weekdays' group which contains all measurements taken on weekdays.
 * There is also a 'weekend' group which contains all measurements taken on weekends.
 */
public class HBaseProfilerClientTest {

  private static final String tableName = "profiler";
  private static final String columnFamily = "P";
  private static final long periodDuration = 15;
  private static final TimeUnit periodUnits = TimeUnit.MINUTES;
  private static final int periodsPerHour = 4;

  private HBaseProfilerClient client;
  private StellarStatefulExecutor executor;
  private MockHTable table;

  @Before
  public void setup() throws Exception {
    table = new MockHTable(tableName, columnFamily);
    executor = new DefaultStellarStatefulExecutor();

    // writes values to be read during testing
    long periodDurationMillis = periodUnits.toMillis(periodDuration);
    RowKeyBuilder rowKeyBuilder = new SaltyRowKeyBuilder();
    ColumnBuilder columnBuilder = new ValueOnlyColumnBuilder(columnFamily);
    client = new HBaseProfilerClient(table, rowKeyBuilder, columnBuilder, periodDurationMillis);
  }

  @After
  public void tearDown() {
    table.clear();
  }

  @Test
  public void shouldFetchMeasurement() {
    final String profile = "profile1";
    final String entity = "entity1";
    final int expectedValue = 2302;
    final int hours = 2;
    final int count = hours * periodsPerHour + 1;
    final long startTime = System.currentTimeMillis() - TimeUnit.HOURS.toMillis(hours);

    // create measurements in the group 'weekdays'
    ProfileMeasurement weekday = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekdays"));
    List<ProfileMeasurement> weekdays = copy(weekday, count, val -> expectedValue);
    client.put(weekdays);

    // create measurements in the group 'weekends'
    ProfileMeasurement weekend = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekends"));
    List<ProfileMeasurement> weekends = copy(weekend, count, val -> 0);
    client.put(weekends);

    long end = System.currentTimeMillis();
    long start = end - TimeUnit.HOURS.toMillis(2);
    {
      //validate "weekday" results
      List<Object> groups = Arrays.asList("weekdays");
      List<ProfileMeasurement> results = client.fetch(Integer.class, profile, entity, groups, start, end, Optional.empty());
      assertEquals(count, results.size());
      results.forEach(actual -> {
        assertEquals(profile, actual.getProfileName());
        assertEquals(entity, actual.getEntity());
        assertEquals(groups, actual.getGroups());
        assertEquals(expectedValue, actual.getProfileValue());
      });
    }
    {
      //validate "weekend" results
      List<Object> groups = Arrays.asList("weekends");
      List<ProfileMeasurement> results = client.fetch(Integer.class, profile, entity, groups, start, end, Optional.empty());
      assertEquals(count, results.size());
      results.forEach(actual -> {
        assertEquals(profile, actual.getProfileName());
        assertEquals(entity, actual.getEntity());
        assertEquals(groups, actual.getGroups());
        assertEquals(0, actual.getProfileValue());
      });
    }
  }

  @Test
  public void shouldReturnResultFromGroup() throws Exception {
    final String profile = "profile1";
    final String entity = "entity1";
    final int periodsPerHour = 4;
    final int expectedValue = 2302;
    final int hours = 2;
    final int count = hours * periodsPerHour;
    final long endTime = System.currentTimeMillis();
    final long startTime = endTime - TimeUnit.HOURS.toMillis(hours);

    // create measurements in the group 'weekdays'
    ProfileMeasurement weekday = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekdays"));
    List<ProfileMeasurement> weekdays = copy(weekday, count, val -> expectedValue);
    client.put(weekdays);

    // create measurements in the group 'weekends'
    ProfileMeasurement weekend = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekends"));
    List<ProfileMeasurement> weekends = copy(weekend, count, val -> 0);
    client.put(weekends);

    List<Object> groups = Arrays.asList("weekdays");
    List<ProfileMeasurement> results = client.fetch(Integer.class, profile, entity, groups, startTime, endTime, Optional.empty());

    // should only return results from 'weekdays' group
    assertEquals(count, results.size());
    results.forEach(actual -> assertEquals("weekdays", actual.getGroups().get(0)));
  }

  @Test
  public void shouldReturnNothingWhenNoGroup() {
    final String profile = "profile1";
    final String entity = "entity1";
    final int periodsPerHour = 4;
    final int expectedValue = 2302;
    final int hours = 2;
    final int count = hours * periodsPerHour;
    final long endTime = System.currentTimeMillis();
    final long startTime = endTime - TimeUnit.HOURS.toMillis(hours);

    // create measurements in the group 'weekdays'
    ProfileMeasurement weekday = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekdays"));
    List<ProfileMeasurement> weekdays = copy(weekday, count, val -> expectedValue);
    client.put(weekdays);

    // create measurements in the group 'weekends'
    ProfileMeasurement weekend = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(startTime, periodDuration, periodUnits)
            .withGroups(Arrays.asList("weekends"));
    List<ProfileMeasurement> weekends = copy(weekend, count, val -> 0);
    client.put(weekends);

    // should return no results when the group does not exist
    List<Object> groups = Arrays.asList("does-not-exist");
    List<ProfileMeasurement> results = client.fetch(Integer.class, profile, entity, groups, startTime, endTime, Optional.empty());
    assertEquals(0, results.size());
  }

  @Test
  public void shouldReturnNothingWhenNoData() {
    final String profile = "profile1";
    final String entity = "entity1";
    final int hours = 2;
    int numberToWrite = hours * periodsPerHour;
    final List<Object> group = Arrays.asList("weekends");
    final long measurementTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);

    // write some data with a timestamp of s1 day ago
    ProfileMeasurement prototype = new ProfileMeasurement()
            .withProfileName("profile1")
            .withEntity("entity1")
            .withGroups(group)
            .withPeriod(measurementTime, periodDuration, periodUnits);
    List<ProfileMeasurement> copies = copy(prototype, numberToWrite, val -> 1000);
    client.put(copies);

    // should return no results when [start,end] is long after when test data was written
    final long endFetchAt = System.currentTimeMillis();
    final long startFetchAt = endFetchAt - TimeUnit.MILLISECONDS.toMillis(30);
    List<ProfileMeasurement> results = client.fetch(Integer.class, profile, entity, group, startFetchAt, endFetchAt, Optional.empty());
    assertEquals(0, results.size());
  }

  @Test
  public void shouldPutMeasurement() {
    final long measurementTime = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(1);
    ProfileMeasurement expected = new ProfileMeasurement()
            .withProfileName("profile1")
            .withEntity("entity1")
            .withPeriod(measurementTime, periodDuration, periodUnits)
            .withProfileValue(2342);

    // write the measurement
    int count = client.put(Collections.singletonList(expected));
    assertEquals(1, count);

    // read the measurement
    List<Object> groups = Collections.emptyList();
    List<ProfilePeriod> periods = Collections.singletonList(expected.getPeriod());
    List<ProfileMeasurement> actuals = client.fetch(Integer.class, "profile1", "entity1", groups, periods, Optional.empty());

    // validate
    assertEquals(1, actuals.size());
    assertEquals(expected, actuals.get(0));
  }
}