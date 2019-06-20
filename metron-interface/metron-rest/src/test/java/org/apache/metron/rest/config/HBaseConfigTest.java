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
package org.apache.metron.rest.config;

import static org.apache.metron.rest.user.UserSettingsClient.USER_SETTINGS_HBASE_CF;
import static org.apache.metron.rest.user.UserSettingsClient.USER_SETTINGS_HBASE_TABLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.common.configuration.EnrichmentConfigurations;
import org.apache.metron.hbase.client.FakeHBaseClient;
import org.apache.metron.hbase.client.FakeHBaseClientCreator;
import org.apache.metron.hbase.client.HBaseClient;
import org.apache.metron.hbase.client.HBaseClientCreator;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.apache.metron.hbase.mock.MockHBaseConnectionFactory;
import org.apache.metron.rest.service.GlobalConfigService;
import org.apache.metron.rest.user.UserSettingsClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Table.class, HBaseConfiguration.class, HBaseConfig.class})
public class HBaseConfigTest {

  private HBaseConnectionFactory hBaseConnectionFactory;
  private HBaseClientCreator hBaseClientCreator;
  private HBaseConfiguration hBaseConfiguration;
  private Configuration configuration;
  private GlobalConfigService globalConfigService;
  private HBaseConfig hBaseConfig;
  private Connection connection;
  private Table table;

  @Before
  public void setUp() throws IOException {
    connection = mock(Connection.class);
    table = mock(Table.class);
    hBaseConnectionFactory = mock(HBaseConnectionFactory.class);
    configuration = mock(Configuration.class);
    hBaseConfiguration = mock(HBaseConfiguration.class);
    hBaseClientCreator = mock(FakeHBaseClientCreator.class);
    globalConfigService = mock(GlobalConfigService.class);
    hBaseConfig = new HBaseConfig(globalConfigService, hBaseConnectionFactory, hBaseConfiguration, hBaseClientCreator);
    mockStatic(HBaseConfiguration.class);
    when(HBaseConfiguration.create()).thenReturn(configuration);
  }

  @Test
  public void userSettingsShouldBeCreated() throws Exception {
    final String expectedTable = "hbase-table-name";
    final String expectedColumnFamily = "hbase-column-family";
    when(globalConfigService.get()).thenReturn(new HashMap<String, Object>() {{
      put(USER_SETTINGS_HBASE_TABLE, expectedTable);
      put(USER_SETTINGS_HBASE_CF, expectedColumnFamily);
    }});

    // connection factory needs to return the mock connection
    when(hBaseConnectionFactory.createConnection(any()))
            .thenReturn(connection);

    // connection should return the table, if the expected table name is used
    when(connection.getTable(eq(TableName.valueOf(expectedTable))))
            .thenReturn(table);

    UserSettingsClient client = hBaseConfig.userSettingsClient();
    Assert.assertNotNull(client);
  }

  @Test
  public void hBaseClientShouldBeCreated() throws Exception {
    final String expectedTableName = "some_table_name";
    when(globalConfigService.get()).thenReturn(new HashMap<String, Object>() {{
      put(EnrichmentConfigurations.TABLE_NAME, expectedTableName);
    }});

    FakeHBaseClient expected = new FakeHBaseClient();
    when(hBaseClientCreator.create(eq(hBaseConnectionFactory), eq(hBaseConfiguration), eq(expectedTableName)))
            .thenReturn(expected);

    HBaseClient client = hBaseConfig.hBaseClient();
    Assert.assertEquals(expected, client);
  }
}
