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
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.function.Supplier;

/**
 * Establishes a {@link Connection} to HBase.
 */
public class HBaseConnectionFactory {

  public Connection createConnection(Configuration configuration) throws IOException {
    return ConnectionFactory.createConnection(configuration);
  }

  public static HBaseConnectionFactory getConnectionFactory(String factoryImpl) {
    return getConnectionFactory(factoryImpl, () -> new HBaseConnectionFactory());
  }

  public static HBaseConnectionFactory getConnectionFactory(String factoryImpl,
                                                            Supplier<HBaseConnectionFactory> defaultImpl) {
    if(factoryImpl == null || factoryImpl.length() == 0 || factoryImpl.charAt(0) == '$') {
      return defaultImpl.get();
    } else {
      try {
        Class<? extends HBaseConnectionFactory> clazz =
                (Class<? extends HBaseConnectionFactory>) Class.forName(factoryImpl);
        return clazz.getConstructor().newInstance();
      } catch (InstantiationException e) {
        throw new IllegalStateException("Unable to instantiate connector.", e);
      } catch (IllegalAccessException e) {
        throw new IllegalStateException("Unable to instantiate connector: illegal access", e);
      } catch (InvocationTargetException e) {
        throw new IllegalStateException("Unable to instantiate connector", e);
      } catch (NoSuchMethodException e) {
        throw new IllegalStateException("Unable to instantiate connector: no such method", e);
      } catch (ClassNotFoundException e) {
        throw new IllegalStateException("Unable to instantiate connector: class not found", e);
      }
    }
  }
}
