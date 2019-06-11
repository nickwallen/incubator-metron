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
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.common.configuration.EnrichmentConfigurations;
import org.apache.metron.rest.service.GlobalConfigService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Table.class, HBaseConfiguration.class, HBaseConfig.class})
public class HBaseConfigTest {

  private GlobalConfigService globalConfigService;
  private HBaseConfig hBaseConfig;

  @Before
  public void setUp() throws Exception {
    globalConfigService = mock(GlobalConfigService.class);
//    hBaseConfig = new HBaseConfig(globalConfigService);
    mockStatic(HBaseConfiguration.class);
  }

  @Test
  public void userSettingsTableShouldBeReturnedFromGlobalConfigByDefault() throws Exception {
    when(globalConfigService.get()).thenReturn(new HashMap<String, Object>() {{
      put(USER_SETTINGS_HBASE_TABLE, "global_config_user_settings_table");
      put(USER_SETTINGS_HBASE_CF, "global_config_user_settings_cf");
    }});
//    HTableProvider htableProvider = mock(HTableProvider.class);
//    whenNew(HTableProvider.class).withNoArguments().thenReturn(htableProvider);
    Configuration configuration = mock(Configuration.class);
    when(HBaseConfiguration.create()).thenReturn(configuration);

    hBaseConfig.userSettingsClient();
//    verify(htableProvider).getTable(configuration, "global_config_user_settings_table");
//    verifyZeroInteractions(htableProvider);
  }

  @Test
  public void hBaseClientShouldBeCreatedWithDefaultProvider() throws Exception {
    when(globalConfigService.get()).thenReturn(new HashMap<String, Object>() {{
      put(EnrichmentConfigurations.TABLE_NAME, "enrichment_list_hbase_table_name");
    }});
    Assert.assertNotNull(hBaseConfig.hBaseClient());
  }

  @Test
  public void hBaseClientShouldBeCreatedWithSpecifiedProvider() throws Exception {
    when(globalConfigService.get()).thenReturn(new HashMap<String, Object>() {{
//      put(EnrichmentConfigurations.TABLE_PROVIDER, MockHBaseTableProvider.class.getName());
      put(EnrichmentConfigurations.TABLE_NAME, "enrichment_list_hbase_table_name");
    }});
    Assert.assertNotNull(hBaseConfig.hBaseClient());
  }

}
