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
package org.apache.metron.rest.user;


import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.apache.metron.rest.user.UserSettingsClient.USER_SETTINGS_HBASE_CF;
import static org.apache.metron.rest.user.UserSettingsClient.USER_SETTINGS_HBASE_TABLE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UserSettingsClientTest {

  @Rule
  public final ExpectedException exception = ExpectedException.none();
  private static ThreadLocal<ObjectMapper> _mapper = ThreadLocal.withInitial(() ->
          new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL));

  private static final String tableName = "some_table";
  private static final String columnFamily = "cf";
  private static byte[] cf = Bytes.toBytes(columnFamily);

  private Table userSettingsTable;
  private Supplier<Map<String, Object>> globalConfigSupplier;
  private UserSettingsClient userSettingsClient;
  private HBaseConnectionFactory connectionFactory;
  private Configuration configuration;
  private Connection connection;

  @Before
  public void setUp() throws Exception {
    // the connection factory should return a mock connection
    connectionFactory = mock(HBaseConnectionFactory.class);
    connection = mock(Connection.class);
    when(connectionFactory.createConnection(eq(configuration)))
            .thenReturn(connection);

    // the connection should return the mock table
    userSettingsTable = mock(Table.class);
    when(connection.getTable(eq(TableName.valueOf(tableName))))
            .thenReturn(userSettingsTable);

    globalConfigSupplier = () -> new HashMap<String, Object>() {{
      put(USER_SETTINGS_HBASE_TABLE, tableName);
      put(USER_SETTINGS_HBASE_CF, columnFamily);
    }};

    userSettingsClient = new UserSettingsClient(globalConfigSupplier, connectionFactory, configuration);
    userSettingsClient.init();
  }

  @Test
  public void shouldFindUserSetting() throws Exception {
    createMockSetting("user1", "timezone", "MST");
    Optional<String> userSetting = userSettingsClient.findOne("user1", "timezone");
    assertEquals("MST", userSetting.get());
  }

  @Test
  public void shouldNotFindNonExistentUser() throws Exception {
    createMockSetting("user1", "timezone", "MST");
    Optional<String> userSetting = userSettingsClient.findOne("missingUser", "timezone");
    assertFalse(userSetting.isPresent());
  }

  @Test
  public void shouldNotFindNonExistentSetting() throws Exception {
    createMockSetting("user1", "timezone", "MST");
    Optional<String> userSetting = userSettingsClient.findOne("user1", "language");
    assertFalse(userSetting.isPresent());
  }

  @Test
  public void shouldFindAllSettingsByType() throws Exception {
    List<Result> settings = Arrays.asList(
            createMockSetting("user1", "timezone", "EST"),
            createMockSetting("user2", "timezone", "PST"));

    ResultScanner resultScanner = mock(ResultScanner.class);
    when(resultScanner.iterator())
            .thenReturn(settings.iterator());
    when(userSettingsTable.getScanner(any(Scan.class)))
            .thenReturn(resultScanner);

    Map<String, Optional<String>> actual = userSettingsClient.findAll("timezone");
    assertEquals("EST", actual.get("user1").get());
    assertEquals("PST", actual.get("user2").get());
  }

  private Result createMockSetting(String user, String settingType, String settingValue) throws IOException {
    // create the mock result containing the value of the setting
    Result result = mock(Result.class);
    when(result.getRow())
            .thenReturn(Bytes.toBytes(user));
    when(result.getValue(cf, Bytes.toBytes(settingType)))
            .thenReturn(settingValue.getBytes());

    // ensure the mock table returns the mock result
    Get expectedGet = new Get(user.getBytes());
    expectedGet.addFamily(cf);
    when(userSettingsTable.get(eq(expectedGet)))
            .thenReturn(result);

    return result;
  }
}
