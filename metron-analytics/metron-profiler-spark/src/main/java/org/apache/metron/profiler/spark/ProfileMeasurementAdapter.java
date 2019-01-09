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

import org.apache.metron.common.utils.SerDeUtils;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.profiler.ProfilePeriod;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/*

start-hbase.sh

cd ~/tmp/apache-phoenix-4.14.1-HBase-1.1-bin/bin
./queryserver.py

See https://zeppelin.apache.org/docs/0.7.3/interpreter/jdbc.html#apache-phoenix
Create Phoenix interpreter in Zeppelin using thin client.
  Properties
  default.driver	  org.apache.phoenix.queryserver.client.Driver
  default.url	      jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF
  default.user	    phoenix_user
  default.password	phoenix_password

Dependencies
Artifact
org.apache.phoenix:phoenix-queryserver-client:4.8.0-HBase-1.2		For Phoenix 4.8+

%phoenix
drop table profiler1;

%phoenix
create table profiler1 (
   profileName varchar not null,
   entity varchar not null,
   periodId bigint not null,
   durationMillis bigint,
   profileValue integer,
   constraint pk primary key (profileName, entity, periodId)
 );

spark-submit \
  --class org.apache.metron.profiler.spark.cli.BatchProfilerCLI \
  --properties-file ~/tmp/profiler.properties \
  target/metron-profiler-spark-0.7.1.jar \
  --config ~/tmp/profiler.properties \
  --profiles ~/tmp/profiles.json

%phoenix
select profilename, count(*)
from profiler1
group by profilename;

%phoenix
select entity, count(*) as count
from profiler1
where profilename='hello-world-1'
group by entity order by entity

%phoenix
select periodid, profilevalue
from profiler1
where profilename='hello-world-1' and entity='62.75.195.236'

%phoenix
select periodid, profilevalue
from profiler1
where profilename='${profile}' and entity='${entity}'

*/

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
  private String profileName;

  /**
   * The name of the entity being profiled.
   */
  private String entity;

  /**
   * A monotonically increasing number identifying the period.  The first period is 0
   * and began at the epoch.
   */
  private Long periodId;

  /**
   * The duration of each period in milliseconds.
   */
  private Long durationMillis;

  /**
   * The result of evaluating the profile expression.
   *
   * The `Encoders.bean(Class<T>)` encoder does not handle serialization of type `Object`. This
   * adapter encodes the profile's result as `byte[]` rather than an `Object` to work around this.
   */
  private Integer profileValue;

  public ProfileMeasurementAdapter() {
    // default constructor required for serialization in Spark
  }

  public ProfileMeasurementAdapter(ProfileMeasurement measurement) {
    this.profileName = measurement.getProfileName();
    this.entity = measurement.getEntity();
    this.periodId = measurement.getPeriod().getPeriod();
    this.durationMillis = measurement.getPeriod().getDurationMillis();
    this.profileValue = Integer.class.cast(measurement.getProfileValue());
  }

  public ProfileMeasurement toProfileMeasurement() {
    ProfilePeriod period = ProfilePeriod.fromPeriodId(periodId, durationMillis, TimeUnit.MILLISECONDS);
    ProfileMeasurement measurement = new ProfileMeasurement()
            .withProfileName(profileName)
            .withEntity(entity)
            .withPeriod(period)
            .withProfileValue(profileValue);
    return measurement;
  }

  public String getProfileName() {
    return profileName;
  }

  public void setProfileName(String profileName) {
    this.profileName = profileName;
  }

  public String getEntity() {
    return entity;
  }

  public void setEntity(String entity) {
    this.entity = entity;
  }

  public Long getPeriodId() {
    return periodId;
  }

  public void setPeriodId(Long periodId) {
    this.periodId = periodId;
  }

  public Long getDurationMillis() {
    return durationMillis;
  }

  public void setDurationMillis(Long durationMillis) {
    this.durationMillis = durationMillis;
  }

  public Integer getProfileValue() {
    return profileValue;
  }

  public void setProfileValue(Integer profileValue) {
    this.profileValue = profileValue;
  }
}
