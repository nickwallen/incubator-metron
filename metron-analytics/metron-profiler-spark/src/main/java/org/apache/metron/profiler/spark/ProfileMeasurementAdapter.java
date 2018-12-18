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
package org.apache.metron.profiler.spark;

import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.ProfilePeriod;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * An adapter for the {@link ProfileMeasurement} class so that the data
 * can be serialized as required by Spark.
 *
 * <p>The `Encoders.bean(Class<T>)` encoder does not handle serialization of type `Object` well. This
 * adapter encodes the profile's result as byte[] rather than an Object to work around this.
 */
public class ProfileMeasurementAdapter implements Serializable {

  /**
   * The name of the profile that this measurement is associated with.
   */
  private String profile;

  /**
   * The name of the entity being profiled.
   */
  private String entity;

  /**
   * A monotonically increasing number identifying the period.  The first period is 0
   * and began at the epoch.
   */
  private Long period;

  /**
   * The duration of each period in milliseconds.
   */
  private Long duration;

  /**
   * The result of evaluating the profile expression.
   *
   * The `Encoders.bean(Class<T>)` encoder does not handle serialization of type `Object`. This
   * adapter encodes the profile's result as `byte[]` rather than an `Object` to work around this.
   */
  private Object result;

  public ProfileMeasurementAdapter() {
    // default constructor required for serialization in Spark
  }

  public ProfileMeasurementAdapter(ProfileMeasurement measurement) {
    this.profile = measurement.getProfileName();
    this.entity = measurement.getEntity();
    this.period = measurement.getPeriod().getPeriod();
    this.duration = measurement.getPeriod().getDurationMillis();
    this.result = measurement.getProfileValue();
  }

  public ProfileMeasurement toProfileMeasurement() {
    ProfilePeriod period = ProfilePeriod.fromPeriodId(this.period, duration, TimeUnit.MILLISECONDS);
    ProfileMeasurement measurement = new ProfileMeasurement()
            .withProfileName(profile)
            .withEntity(entity)
            .withPeriod(period)
            .withProfileValue(result);
    return measurement;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile(String profile) {
    this.profile = profile;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public Long getPeriod() {
    return period;
  }

  public void setPeriod(Long period) {
    this.period = period;
  }

  public Long getDuration() {
    return duration;
  }

  public void setDuration(Long duration) {
    this.duration = duration;
  }

  public Object getResult() {
    return result;
  }

  public void setResult(Object result) {
    this.result = result;
  }
}
