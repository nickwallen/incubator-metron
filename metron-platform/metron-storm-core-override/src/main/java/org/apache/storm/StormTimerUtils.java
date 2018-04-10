/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm;

import java.util.UUID;

/**
 * These functions were backported from org.apache.storm.utils.Utils in v1.1.0 of Apache Storm.
 */
public class StormTimerUtils {

  /**
   * Checks if a throwable is an instance of a particular class
   * @param klass The class you're expecting
   * @param throwable The throwable you expect to be an instance of klass
   * @return true if throwable is instance of klass, false otherwise.
   */
  public static boolean exceptionCauseIsInstanceOf(Class klass, Throwable throwable) {
    Throwable t = throwable;
    while (t != null) {
      if (klass.isInstance(t)) {
        return true;
      }
      t = t.getCause();
    }
    return false;
  }

  public static String uuid() {
    return UUID.randomUUID().toString();
  }
}
