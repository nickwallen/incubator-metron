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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.metron.hbase.client.HBaseConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.function.Supplier;

public class UserSettingsClient implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  public static final String USER_SETTINGS_HBASE_TABLE = "user.settings.hbase.table";
  public static final String USER_SETTINGS_HBASE_CF = "user.settings.hbase.cf";

  private Table userSettingsTable;
  private byte[] cf;
  private Supplier<Map<String, Object>> globalConfigSupplier;
  private HBaseConnectionFactory connectionFactory;
  private Configuration configuration;
  private Connection connection;

  public UserSettingsClient(Supplier<Map<String, Object>> globalConfigSupplier,
                            HBaseConnectionFactory connectionFactory,
                            Configuration configuration) {
    this.globalConfigSupplier = globalConfigSupplier;
    this.connectionFactory = connectionFactory;
    this.configuration = configuration;
  }

  public synchronized void init() {
    if (this.userSettingsTable == null) {
      Map<String, Object> globalConfig = getGlobals();
      String table = getTableName(globalConfig);
      this.cf = getColumnFamily(globalConfig).getBytes();
      try {
        connection = connectionFactory.createConnection(configuration);
        userSettingsTable = connection.getTable(TableName.valueOf(table));

      } catch (IOException e) {
        throw new IllegalStateException("Unable to initialize HBaseDao: " + e.getMessage(), e);
      }
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if(userSettingsTable != null) {
      userSettingsTable.close();
    }
    if(connection != null) {
      connection.close();
    }
  }

  private Map<String, Object> getGlobals() {
    Map<String, Object> globalConfig = globalConfigSupplier.get();
    if(globalConfig == null) {
      throw new IllegalStateException("Cannot find the global config.");
    }
    return globalConfig;
  }

  private static String getTableName(Map<String, Object> globalConfig) {
    String table = (String) globalConfig.get(USER_SETTINGS_HBASE_TABLE);
    if(table == null) {
      throw new IllegalStateException("You must configure " + USER_SETTINGS_HBASE_TABLE + "in the global config.");
    }
    return table;
  }

  private static String getColumnFamily(Map<String, Object> globalConfig) {
    String cf = (String) globalConfig.get(USER_SETTINGS_HBASE_CF);
    if(cf == null) {
      throw new IllegalStateException("You must configure " + USER_SETTINGS_HBASE_CF + " in the global config.");
    }

    return cf;
  }

  protected Table getTable() {
    if(userSettingsTable == null) {
      init();
    }
    return userSettingsTable;
  }

  public Map<String, String> findOne(String user) throws IOException {
    Result result = getResult(user);
    return getAllUserSettings(result);
  }

  public Optional<String> findOne(String user, String type) throws IOException {
    Result result = getResult(user);
    return getUserSettings(result, type);
  }

  public Map<String, Map<String, String>> findAll() throws IOException {
    Scan scan = new Scan();
    ResultScanner results = getTable().getScanner(scan);
    Map<String, Map<String, String>> allUserSettings = new HashMap<>();
    for (Result result : results) {
      allUserSettings.put(new String(result.getRow()), getAllUserSettings(result));
    }
    return allUserSettings;
  }

  public Map<String, Optional<String>> findAll(String type) throws IOException {
    Scan scan = new Scan();
    ResultScanner results = getTable().getScanner(scan);
    Map<String, Optional<String>> allUserSettings = new HashMap<>();
    for (Result result : results) {
      allUserSettings.put(new String(result.getRow()), getUserSettings(result, type));
    }
    return allUserSettings;
  }

  public void save(String user, String type, String userSettings) throws IOException {
    byte[] rowKey = Bytes.toBytes(user);
    Put put = new Put(rowKey);
    put.addColumn(cf, Bytes.toBytes(type), Bytes.toBytes(userSettings));
    getTable().put(put);
  }

  public void delete(String user) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(user));
    getTable().delete(delete);
  }

  public void delete(String user, String type) throws IOException {
    Delete delete = new Delete(Bytes.toBytes(user));
    delete.addColumn(cf, Bytes.toBytes(type));
    getTable().delete(delete);
  }

  private Result getResult(String user) throws IOException {
    byte[] rowKey = Bytes.toBytes(user);
    Get get = new Get(rowKey);
    get.addFamily(cf);
    return getTable().get(get);
  }

  private Optional<String> getUserSettings(Result result, String type) throws IOException {
    Optional<String> userSettings = Optional.empty();
    if (result != null) {
      byte[] value = result.getValue(cf, Bytes.toBytes(type));
      if (value != null) {
        userSettings = Optional.of(new String(value, StandardCharsets.UTF_8));
      }
    }
    return userSettings;
  }

  public Map<String, String> getAllUserSettings(Result result) {
    if (result == null) {
      return new HashMap<>();
    }
    NavigableMap<byte[], byte[]> columns = result.getFamilyMap(cf);
    if(columns == null || columns.size() == 0) {
      return new HashMap<>();
    }
    Map<String, String> userSettingsMap = new HashMap<>();
    for(Map.Entry<byte[], byte[]> column: columns.entrySet()) {
      userSettingsMap.put(new String(column.getKey(), StandardCharsets.UTF_8), new String(column.getValue(), StandardCharsets.UTF_8));
    }
    return userSettingsMap;
  }
}
