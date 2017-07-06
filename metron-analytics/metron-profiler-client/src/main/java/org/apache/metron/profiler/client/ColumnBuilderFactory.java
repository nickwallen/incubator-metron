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

import org.apache.commons.beanutils.PropertyUtils;
import org.apache.metron.common.utils.ReflectionUtils;
import org.apache.metron.profiler.hbase.ColumnBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.apache.metron.profiler.client.stellar.ProfilerConfig.PROFILER_COLUMN_FAMILY;

/**
 * A factory for creating ColumnBuilder instances.
 */
public class ColumnBuilderFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ColumnBuilderFactory.class);

  /**
   * Create a ColumnBuilder.
   * @param className The class name of the ColumnBuilder.
   * @param global The global properties used to configure the ColumnBuilder.
   * @return A new ColumnBuilder instance.
   */
  public static ColumnBuilder create(String className, Map<String, Object> global) {

    ColumnBuilder builder = ReflectionUtils.createInstance(className);
    try {

      // set the column family
      if(PropertyUtils.isWriteable(builder, "columnFamily")) {
        String columnFamily = PROFILER_COLUMN_FAMILY.get(global, String.class);
        PropertyUtils.setProperty(builder, "columnFamily", columnFamily);
      }

    } catch(IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      LOG.warn("Unable to set property on ColumnBuilder", e);
    }

    return builder;
  }

}
