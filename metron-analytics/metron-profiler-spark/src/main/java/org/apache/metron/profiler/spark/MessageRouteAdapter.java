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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.profiler.MessageRoute;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.IOException;
import java.io.Serializable;

public class MessageRouteAdapter implements Serializable {

  private String profileName;
  private String entity;
  private String message;
  private String profile;
  private Long timestamp;

  public MessageRouteAdapter() {
    // default constructor required for encoders and serialization in Spark
  }

  public MessageRouteAdapter(MessageRoute route) {
    this.profileName = route.getProfileDefinition().getProfile();
    this.entity = route.getEntity();
    this.message = route.getMessage().toJSONString();
    this.timestamp = route.getTimestamp();
    try {
      this.profile = route.getProfileDefinition().toJSON();
    } catch(JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public MessageRoute toMessageRoute() {
    JSONParser parser = new JSONParser();
    try {
      ProfileConfig profileDefinition = ProfileConfig.fromJSON(profile);
      JSONObject parsedMessage = (JSONObject) parser.parse(message);
      return new MessageRoute(profileDefinition, entity, parsedMessage, timestamp);

    } catch(IOException | ParseException e) {
      throw new RuntimeException(e);
    }
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

  public String getMessage() {
    return message;
  }

  public void setMessage(String message) {
    this.message = message;
  }

  public String getProfile() {
    return profile;
  }

  public void setProfile(String profile) {
    this.profile = profile;
  }

  public Long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(Long timestamp) {
    this.timestamp = timestamp;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MessageRouteAdapter that = (MessageRouteAdapter) o;
    return new EqualsBuilder()
            .append(entity, that.entity)
            .append(message, that.message)
            .append(profile, that.profile)
            .append(timestamp, that.timestamp)
            .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
            .append(entity)
            .append(message)
            .append(profile)
            .append(timestamp)
            .toHashCode();
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this)
            .append("entity", entity)
            .append("message", message)
            .append("profile", profile)
            .append("timestamp", timestamp)
            .toString();
  }
}
