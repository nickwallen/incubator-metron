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
package org.apache.metron.hbase.client;

import org.apache.hadoop.conf.Configuration;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Responsible for creating an {@link HBaseClient}.
 */
public interface HBaseClientCreator {

  /**
   * @param factory The connection factory for creating connections to HBase.
   * @param configuration The HBase configuration.
   * @param tableName The name of the HBase table.
   * @return An {@link HBaseClient}.
   */
  HBaseClient create(HBaseConnectionFactory factory, Configuration configuration, String tableName);

  /**
   * Instantiates a new {@link HBaseClientCreator} by class name.
   *
   * @param creatorImpl The class name of the {@link HBaseClientCreator} to instantiate.
   * @param defaultImpl The default instance to instantiate if the creatorImpl is invalid.
   * @return A new {@link HBaseClientCreator}.
   */
  static HBaseClientCreator newInstance(String creatorImpl, Supplier<HBaseClientCreator> defaultImpl) {
    if(creatorImpl == null || creatorImpl.length() == 0 || creatorImpl.charAt(0) == '$') {
      return defaultImpl.get();
    } else {
      try {
        Class<? extends HBaseClientCreator> clazz = (Class<? extends HBaseClientCreator>) Class.forName(creatorImpl);
        return clazz.getConstructor().newInstance();

      } catch(InstantiationException | IllegalAccessException | InvocationTargetException |
              NoSuchMethodException | ClassNotFoundException e) {
        throw new IllegalStateException("Unable to instantiate connector.", e);
      }
    }
  }
}
