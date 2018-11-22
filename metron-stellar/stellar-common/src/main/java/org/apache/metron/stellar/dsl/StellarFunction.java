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
package org.apache.metron.stellar.dsl;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * The interface used to define a Stellar function.
 */
public interface StellarFunction extends Closeable {

  /**
   * Apply the Stellar function to the function arguments.
   *
   * @param args The function arguments.
   * @param context The execution context.
   * @return The result of executing the function.
   * @throws ParseException
   */
  Object apply(List<Object> args, Context context) throws ParseException;

  /**
   * Allows the function to initialize resources.
   *
   * @param context The execution context.
   */
  default void initialize(Context context) {
    // no initialization needed
  }

  /**
   * Indicates if the function has been initialized.
   *
   * @return True if the function has been initialized. Otherwise, false.
   */
  default boolean isInitialized() {
    // no initialization needed
    return true;
  }

  /**
   * Allows the function to close any resources.
   * @throws IOException
   */
  @Override
  default void close() throws IOException {
    // do nothing
  }
}
