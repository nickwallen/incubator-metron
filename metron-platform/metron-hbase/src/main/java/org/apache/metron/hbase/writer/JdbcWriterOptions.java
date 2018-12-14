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
package org.apache.metron.hbase.writer;

import org.apache.metron.common.configuration.ConfigOption;
import org.apache.metron.stellar.common.utils.ConversionUtils;

import java.util.Map;

/**
 * Defines the options available to configure the {@link JdbcWriter}.
 */
public enum JdbcWriterOptions implements ConfigOption {

  JDBC_URL("jdbc.url", null),
  JDBC_DRIVER("jdbc.driver", null),
  JDBC_USERNAME("jdbc.username", null),
  JDBC_PASSWORD("jdbc.password", null);

  private String key;
  private Object defaultValue;

  JdbcWriterOptions(String key, Object defaultValue) {
    this.key = key;
    this.defaultValue = defaultValue;
  }

  @Override
  public String getKey() {
    return key;
  }

  public <T> T getOrDefault(Map<String, Object> map, Class<T> clazz) {
    T value;
    if(containsOption(map)) {
      // the value is defined
      value = this.get(map, clazz);

    } else if (clazz.isInstance(defaultValue)) {
      // cast the default value
      value = clazz.cast(defaultValue);

    } else {
      // convert the default value
      value = ConversionUtils.convert(defaultValue, clazz);
    }

    return value;
  }
}
