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

package org.apache.metron.profiler;

import org.apache.commons.beanutils.BeanMap;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.dsl.Context;
import org.apache.metron.common.dsl.ParseException;
import org.apache.metron.common.dsl.StellarFunctions;
import org.apache.metron.profiler.stellar.DefaultStellarExecutor;
import org.apache.metron.profiler.stellar.StellarExecutor;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * Responsible for building and maintaining a Profile.
 *
 * One or more messages are applied to the Profile with `apply` and a profile measurement is
 * produced by calling `flush`.
 *
 * Any one instance is responsible only for building the profile for a specific [profile, entity]
 * pairing.  There will exist many instances, one for each [profile, entity] pair that exists
 * within the incoming telemetry data applied to the profile.
 */
public class ProfileBuilder {

  protected static final Logger LOG = LoggerFactory.getLogger(ProfileBuilder.class);

  /**
   * A ProfileMeasurement is created and emitted each window period.  A Profile
   * itself is composed of many ProfileMeasurements.
   */
  private ProfileMeasurement measurement;

  /**
   * The definition of the Profile that the bolt is building.
   */
  private ProfileConfig definition;

  /**
   * Executes Stellar code and maintains state across multiple invocations.
   */
  private StellarExecutor executor;

  /**
   * Has the profile been initialized?
   */
  private boolean isInitialized;

  /**
   * Use the ProfileBuilder.Builder to create a new ProfileBuilder.
   */
  private ProfileBuilder(ProfileConfig definition,
                         String entity,
                         long whenMillis,
                         long periodDurationMillis,
                         CuratorFramework client,
                         Map<String, Object> global) {

    this.isInitialized = false;
    this.definition = definition;
    this.measurement = new ProfileMeasurement(
            definition.getProfile(),
            entity,
            whenMillis,
            periodDurationMillis,
            TimeUnit.MILLISECONDS);
    this.executor = new DefaultStellarExecutor();
    Context context = new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> client)
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
            .build();
    StellarFunctions.initialize(context);
    this.executor.setContext(context);
  }

  /**
   * Apply a message to the profile.
   * @param message The message to apply.
   */
  public void apply(JSONObject message) {

    if(!isInitialized()) {
      assign(definition.getInit(), message, "init");
      isInitialized = true;
    }

    assign(definition.getUpdate(), message, "update");
  }

  /**
   * Flush the Profile.
   *
   * Completes and emits the ProfileMeasurement.  Clears all state in preparation for
   * the next window period.
   * @return Returns the completed profile measurement.
   */
  public ProfileMeasurement flush() {
    LOG.debug("Flushing profile: profile={}, entity={}", measurement.getProfileName(), measurement.getEntity());

    // execute the 'result' expression
    Object value = execute(definition.getResult(), new JSONObject(), "result");
    measurement.setValue(value);

    // execute the 'groupBy' expression(s) - allow each expression to refer to the fields of the ProfileMeasurement
    List<Object> groups = execute(definition.getGroupBy(), new BeanMap(measurement), "groupBy");
    measurement.setGroups(groups);

    // execute the 'tickUpdate' expression(s)
    assign(definition.getTickUpdate(), Collections.singletonMap("result", value),"tickUpdate");

    // save a copy of current state then clear it to prepare for the next window
    Map<String, Object> state = executor.getState();
    executor.clearState();

    // the 'tickUpdate' state is not flushed - make sure to bring that state along to the next period
    definition.getTickUpdate().forEach((var, expr) -> {
      Object val = state.get(var);
      executor.assign(var, val);
    });

    isInitialized = false;
    return measurement;
  }

  /**
   * Executes an expression contained within the profile definition.
   * @param expression The expression to execute.
   * @param transientState Additional transient state provided to the expression.
   * @param expressionType The type of expression; init, update, result.  Provides additional context if expression execution fails.
   * @return The result of executing the expression.
   */
  private Object execute(String expression, Map<String, Object> transientState, String expressionType) {
    Object result = null;

    List<Object> allResults = execute(Collections.singletonList(expression), transientState, expressionType);
    if(allResults.size() > 0) {
      result = allResults.get(0);
    }

    return result;
  }

  /**
   * Executes a set of expressions whose results need to be assigned to a variable.
   * @param expressions Maps the name of a variable to the expression whose result should be assigned to it.
   * @param transientState Additional transient state provided to the expression.
   * @param expressionType The type of expression; init, update, result.  Provides additional context if expression execution fails.
   */
  private void assign(Map<String, String> expressions, Map<String, Object> transientState, String expressionType) {
    try {

      // execute each of the 'update' expressions
      MapUtils.emptyIfNull(expressions)
              .forEach((var, expr) -> executor.assign(var, expr, transientState));

    } catch(ParseException e) {

      // make it brilliantly clear that one of the 'update' expressions is bad
      String msg = format(
              "Bad '%s' expression: %s, profile=%s, entity=%s",
              expressionType,
              e.getMessage(),
              measurement.getProfileName(),
              measurement.getEntity());
      throw new ParseException(msg, e);
    }
  }

  /**
   * Executes the expressions contained within the profile definition.
   * @param expressions A list of expressions to execute.
   * @param transientState Additional transient state provided to the expressions.
   * @param expressionType The type of expression; init, update, result.  Provides additional context if expression execution fails.
   * @return The result of executing each expression.
   */
  private List<Object> execute(List<String> expressions, Map<String, Object> transientState, String expressionType) {
    List<Object> results = new ArrayList<>();

    try {
      ListUtils.emptyIfNull(expressions)
              .forEach((expr) -> results.add(executor.execute(expr, transientState, Object.class)));

    } catch (Throwable e) {
      String msg = format("Bad '%s' expression: %s, profile=%s, entity=%s",
              expressionType,
              e.getMessage(),
              measurement.getProfileName(),
              measurement.getEntity());
      throw new ParseException(msg, e);
    }

    return results;
  }

  /**
   * Returns the current value of a variable.
   * @param variable The name of the variable.
   */
  public Object valueOf(String variable) {
    return executor.getState().get(variable);
  }

  public boolean isInitialized() {
    return isInitialized;
  }

  public ProfileConfig getDefinition() {
    return definition;
  }

  /**
   * A builder used to construct a new ProfileBuilder.
   */
  public static class Builder {

    private ProfileConfig definition;
    private String entity;
    private long periodAt;
    private long periodDurationMillis;
    private CuratorFramework zookeeperClient;
    private Map<String, Object> global;

    /**
     * @param definition The profiler definition.
     */
    public Builder withDefinition(ProfileConfig definition) {
      this.definition = definition;
      return this;
    }

    /**
     * @param entity The name of the entity
     */
    public Builder withEntity(String entity) {
      this.entity = entity;
      return this;
    }

    /**
     * @param periodAtMillis A time within the profile period when the measurement was taken in epoch milliseconds.
     */
    public Builder withPeriodAt(long periodAtMillis) {
      this.periodAt = periodAtMillis;
      return this;
    }

    /**
     * @param duration The duration of each profile period.
     * @param units The units used to specify the duration of the profile period.
     */
    public Builder withPeriodDuration(long duration, TimeUnit units) {
      this.periodDurationMillis = units.toMillis(duration);
      return this;
    }

    /**
     * @param millis The duration of each profile period in milliseconds.
     */
    public Builder withPeriodDurationMillis(long millis) {
      this.periodDurationMillis = millis;
      return this;
    }

    /**
     * @param zookeeperClient The zookeeper client.
     */
    public Builder withZookeeperClient(CuratorFramework zookeeperClient) {
      this.zookeeperClient = zookeeperClient;
      return this;
    }

    /**
     * @param global The global configuration.
     */
    public Builder withGlobalConfiguration(Map<String, Object> global) {

      // TODO how does the profile builder ever seen a global that has been update in zookeeper?

      this.global = global;
      return this;
    }

    /**
     * Construct a ProfileBuilder.
     */
    public ProfileBuilder build() {

      if(definition == null) {
        throw new IllegalArgumentException("missing profiler definition; got null");
      }

      if(StringUtils.isEmpty(entity)) {
        throw new IllegalArgumentException(format("missing entity name; got '%s'", entity));
      }

      return new ProfileBuilder(definition, entity, periodAt, periodDurationMillis, zookeeperClient, global);
    }
  }
}
