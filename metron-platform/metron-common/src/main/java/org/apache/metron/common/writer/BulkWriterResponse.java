/*
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

package org.apache.metron.common.writer;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.storm.tuple.Tuple;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The response returned by a {@link BulkMessageWriter} after completing a bulk write.
 */
public class BulkWriterResponse {

  /**
   * Maps an exception to all of the tuples in error that experienced that exception.
   */
  private Multimap<Throwable, Tuple> errors = ArrayListMultimap.create();

  /**
   * All of the successful tuples.
   */
  private List<Tuple> successes = new ArrayList<>();

  /**
   * Add a tuple to the response that failed to be successfully written by the {@link BulkMessageWriter}.
   *
   * @param error The root cause of the error.
   * @param tuple The tuple that experienced the error.
   */
  public void addError(Throwable error, Tuple tuple) {
    errors.put(error, tuple);
  }

  /**
   * Adds multiple tuples to the response that failed to be successfully written by the {@link BulkMessageWriter}.
   *
   * @param error The root cause of the error.
   * @param tuples All of the tuples that experienced the error.
   */
  public void addAllErrors(Throwable error, Iterable<Tuple> tuples) {
    if(tuples != null) {
      errors.putAll(error, tuples);
    }
  }

  /**
   * Returns true if the response has any number of errors.
   *
   * @return True, if the response has tuples in error.  Otherwise, false.
   */
  public boolean hasErrors() {
    return !errors.isEmpty();
  }

  /**
   * Returns the number of tuples that failed to write.
   *
   * @return The number of failed tuples.
   */
  public int numberOfErrors() {
    return errors.values().size();
  }

  /**
   * Returns the number of successful tuples.
   *
   * @return The number of successful tuples.
   */
  public int numberOfSuccesses() {
    return successes.size();
  }

  /**
   * Adds a tuple to the response that was successfully written by a {@link BulkMessageWriter}.
   *
   * @param success A tuple that was successfully written by a {@link BulkMessageWriter}.
   */
  public void addSuccess(Tuple success) {
    successes.add(success);
  }

  /**
   * Adds multiple tuples to the response that were successfully written by a {@link BulkMessageWriter}.
   *
   * @param allSuccesses The tuples that were successfully written by a {@link BulkMessageWriter}.
   */
  public void addAllSuccesses(Iterable<Tuple> allSuccesses) {
    if(allSuccesses != null) {
      Iterables.addAll(successes, allSuccesses);
    }
  }

  /**
   * Returns all tuples that failed to be written by a {@link BulkMessageWriter}.
   *
   * @return All failed tuples.
   */
  public Map<Throwable, Collection<Tuple>> getErrors() {
    return errors.asMap();
  }

  /**
   * Returns all tuples that were successfully written by a {@link BulkMessageWriter}.
   *
   * @return All successfully written tuples.
   */
  public List<Tuple> getSuccesses() {
    return successes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    BulkWriterResponse that = (BulkWriterResponse) o;
    return new EqualsBuilder()
            .append(errors, that.errors)
            .append(successes, that.successes)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(errors)
            .append(successes)
            .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("errors", errors)
            .append("successes", successes)
            .toString();
  }
}
