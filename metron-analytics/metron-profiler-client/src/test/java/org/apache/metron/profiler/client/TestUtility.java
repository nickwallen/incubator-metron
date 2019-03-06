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

import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.ProfilePeriod;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class TestUtility {

  private TestUtility() {
    // do not instantiate
  }

  /**
   * Creates 'n' sequential, copies of a prototype {@link ProfileMeasurement}.
   * @param prototype The prototype to copy.
   * @param numberOfCopies The number of copies.
   * @param valueGenerator A function that consumes the previous ProfileMeasurement value and produces the next.
   * @return A list containing 'n', sequential copies of the prototype.
   */
  public static  List<ProfileMeasurement> copy(ProfileMeasurement prototype,
                                               int numberOfCopies,
                                               Function<Object, Object> valueGenerator) {
    List<ProfileMeasurement> copies = new ArrayList<>();
    ProfileMeasurement copy = prototype;
    ProfilePeriod period = copy.getPeriod();
    for(int i=0; i<numberOfCopies; i++) {
      // generate the next value that should be written
      Object nextValue = valueGenerator.apply(copy.getProfileValue());

      // write the measurement
      copy = new ProfileMeasurement()
              .withProfileName(prototype.getProfileName())
              .withEntity(prototype.getEntity())
              .withPeriod(period)
              .withGroups(prototype.getGroups())
              .withProfileValue(nextValue);
      copies.add(copy);

      // advance to the next period
      period = copy.getPeriod().next();
    }

    return copies;
  }
}
