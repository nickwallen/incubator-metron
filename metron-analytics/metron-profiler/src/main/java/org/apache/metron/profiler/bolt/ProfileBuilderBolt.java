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

package org.apache.metron.profiler.bolt;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.ConfigurationType;
import org.apache.metron.common.configuration.ConfigurationsUtils;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.configuration.profiler.ProfilerConfigurations;
import org.apache.metron.common.zookeeper.configurations.ConfigurationsUpdater;
import org.apache.metron.common.zookeeper.configurations.ProfilerUpdater;
import org.apache.metron.common.zookeeper.configurations.Reloadable;
import org.apache.metron.profiler.DefaultMessageDistributor;
import org.apache.metron.profiler.MessageRoute;
import org.apache.metron.profiler.ProfileMeasurement;
import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.zookeeper.SimpleEventListener;
import org.apache.metron.zookeeper.ZKCache;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.apache.metron.profiler.bolt.ProfileSplitterBolt.ENTITY_TUPLE_FIELD;
import static org.apache.metron.profiler.bolt.ProfileSplitterBolt.MESSAGE_TUPLE_FIELD;
import static org.apache.metron.profiler.bolt.ProfileSplitterBolt.PROFILE_TUPLE_FIELD;
import static org.apache.metron.profiler.bolt.ProfileSplitterBolt.TIMESTAMP_TUPLE_FIELD;

/**
 * A Storm bolt that is responsible for building a profile.
 *
 * <p>This bolt maintains the state required to build a Profile.  When the window
 * period expires, the data is summarized as a {@link ProfileMeasurement}, all state is
 * flushed, and the {@link ProfileMeasurement} is emitted.
 */
public class ProfileBuilderBolt extends BaseWindowedBolt implements Reloadable {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private OutputCollector collector;

  /**
   * The URL to connect to Zookeeper.
   */
  private String zookeeperUrl;

  /**
   * The Zookeeper client connection.
   */
  protected CuratorFramework zookeeperClient;

  /**
   * The Zookeeper cache.
   */
  protected ZKCache zookeeperCache;

  /**
   * Manages configuration for the Profiler.
   */
  private ProfilerConfigurations configurations;

  /**
   * The duration of each profile period in milliseconds.
   */
  private long periodDurationMillis;

  /**
   * The duration of Storm's window.
   *
   * <p>The Profile duration must be a multiple of Storm's window duration.
   */
  private long windowDurationMillis;

  /**
   * Counts down until the next flush event.
   *
   * <p>There will be at least 1 (most often many) Storm windows for each profile period.
   * When a window of tuples is received from Storm, this counter is decremented.  Once
   * the counter reaches zero, it is time to flush the profiles.
   */
  private long flushCountdown;

  /**
   * If a message has not been applied to a Profile in this number of milliseconds,
   * the Profile will be forgotten and its resources will be cleaned up.
   *
   * <p>WARNING: The TTL must be at least greater than the period duration.
   */
  private long profileTimeToLiveMillis;

  /**
   * The maximum number of {@link MessageRoute} routes that will be maintained by
   * this bolt.  After this value is exceeded, lesser used routes will be evicted
   * from the internal cache.
   */
  private long maxNumberOfRoutes;

  /**
   * Distributes messages to the profile builders.
   */
  private DefaultMessageDistributor messageDistributor;

  /**
   * Parses JSON messages.
   */
  private transient JSONParser parser;

  /**
   * The {@link ProfileMeasurement} values generated by a profile can be written to
   * multiple endpoints like HBase or Kafka.  Each endpoint is handled by a separate
   * {@link ProfileMeasurementEmitter}.
   */
  private List<ProfileMeasurementEmitter> emitters;

  public ProfileBuilderBolt() {
    this.emitters = new ArrayList<>();
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    super.prepare(stormConf, context, collector);

    if(periodDurationMillis <= 0) {
      throw new IllegalArgumentException("expect 'profiler.period.duration' >= 0");
    }
    if(profileTimeToLiveMillis <= 0) {
      throw new IllegalArgumentException("expect 'profiler.ttl' >= 0");
    }
    if(profileTimeToLiveMillis < periodDurationMillis) {
      throw new IllegalArgumentException("expect 'profiler.ttl' >= 'profiler.period.duration'");
    }
    if(maxNumberOfRoutes <= 0) {
      throw new IllegalArgumentException("expect 'profiler.max.routes.per.bolt' > 0");
    }
    if(windowDurationMillis <= 0) {
      throw new IllegalArgumentException("expect 'profiler.window.duration' > 0");
    }
    if(windowDurationMillis > periodDurationMillis) {
      throw new IllegalArgumentException("expect 'profiler.period.duration' >= 'profiler.window.duration'");
    }
    if(periodDurationMillis % windowDurationMillis != 0) {
      throw new IllegalArgumentException("expect 'profiler.period.duration' % 'profiler.window.duration' == 0");
    }

    this.collector = collector;
    this.parser = new JSONParser();
    this.messageDistributor = new DefaultMessageDistributor(periodDurationMillis, profileTimeToLiveMillis, maxNumberOfRoutes);
    this.configurations = new ProfilerConfigurations();
    setupZookeeper();
    reset();
  }

  @Override
  public void cleanup() {
    zookeeperCache.close();
    zookeeperClient.close();
  }

  private void setupZookeeper() {
    try {
      if (zookeeperClient == null) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        zookeeperClient = CuratorFrameworkFactory.newClient(zookeeperUrl, retryPolicy);
      }
      zookeeperClient.start();

      //this is temporary to ensure that any validation passes.
      //The individual bolt will reinitialize stellar to dynamically pull from
      //zookeeper.
      ConfigurationsUtils.setupStellarStatically(zookeeperClient);
      if (zookeeperCache == null) {
        ConfigurationsUpdater<ProfilerConfigurations> updater = createUpdater();
        SimpleEventListener listener = new SimpleEventListener.Builder()
                .with( updater::update, TreeCacheEvent.Type.NODE_ADDED, TreeCacheEvent.Type.NODE_UPDATED)
                .with( updater::delete, TreeCacheEvent.Type.NODE_REMOVED)
                .build();
        zookeeperCache = new ZKCache.Builder()
                .withClient(zookeeperClient)
                .withListener(listener)
                .withRoot(Constants.ZOOKEEPER_TOPOLOGY_ROOT)
                .build();
        updater.forceUpdate(zookeeperClient);
        zookeeperCache.start();
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  protected ConfigurationsUpdater<ProfilerConfigurations> createUpdater() {
    return new ProfilerUpdater(this, this::getConfigurations);
  }

  public ProfilerConfigurations getConfigurations() {
    return configurations;
  }

  @Override
  public void reloadCallback(String name, ConfigurationType type) {
    // nothing to do
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {

    if(emitters.size() == 0) {
      throw new IllegalStateException("At least one destination handler must be defined.");
    }

    // allow each emitter to define its own stream
    emitters.forEach(emitter -> emitter.declareOutputFields(declarer));
  }

  private Context getStellarContext() {

    Map<String, Object> global = getConfigurations().getGlobalConfig();
    return new Context.Builder()
            .with(Context.Capabilities.ZOOKEEPER_CLIENT, () -> zookeeperClient)
            .with(Context.Capabilities.GLOBAL_CONFIG, () -> global)
            .with(Context.Capabilities.STELLAR_CONFIG, () -> global)
            .build();
  }

  @Override
  public void execute(TupleWindow window) {

    LOG.debug("Tuple window contains {} tuple(s), {} expired, {} new",
            CollectionUtils.size(window.get()),
            CollectionUtils.size(window.getExpired()),
            CollectionUtils.size(window.getNew()));

    try {
      doExecute(window);

    } catch (Throwable e) {
      LOG.error("Unexpected error", e);
      collector.reportError(e);
    }
  }

  private void doExecute(TupleWindow window) {

    // handle each tuple in the window
    for(Tuple tuple : window.get()) {
      handleTuple(tuple);
    }

    // is it time to flush?
    if(flushCountdown == 0) {
      flush();
      reset();

    } else {
      // not time to flush yet
      flushCountdown--;
    }
  }

  /**
   * Flush the profiles.
   */
  private void flush() {

    // flush each profile
    List<ProfileMeasurement> measurements = messageDistributor.flush();
    for(ProfileMeasurement measurement: measurements) {

      // allow each 'emitter' to emit the measurement
      for (ProfileMeasurementEmitter emitter : emitters) {
        emitter.emit(measurement, collector);
      }
    }
  }

  /**
   * Resets the flush count-down counter.
   *
   * <p>There must be at least 1 (most often many) Storm windows for each profile period.
   * When a window of tuples is received from Storm, this counter is decremented.  Once
   * the counter reaches zero, it is time to flush the profiles.
   */
  private void reset() {
    flushCountdown = (periodDurationMillis / windowDurationMillis) - 1;
  }

  /**
   * Handles the processing of a single tuple.
   *
   * @param input The tuple containing a telemetry message.
   */
  private void handleTuple(Tuple input) {

    // crack open the tuple
    JSONObject message = getField(MESSAGE_TUPLE_FIELD, input, JSONObject.class);
    ProfileConfig definition = getField(PROFILE_TUPLE_FIELD, input, ProfileConfig.class);
    String entity = getField(ENTITY_TUPLE_FIELD, input, String.class);
    Long timestamp = getField(TIMESTAMP_TUPLE_FIELD, input, Long.class);

    // distribute the message
    MessageRoute route = new MessageRoute(definition, entity);
    messageDistributor.distribute(message, timestamp, route, getStellarContext());
  }

  /**
   * Retrieves an expected field from a Tuple.  If the field is missing an exception is thrown to
   * indicate a fatal error.
   * @param fieldName The name of the field.
   * @param tuple The tuple from which to retrieve the field.
   * @param clazz The type of the field value.
   * @param <T> The type of the field value.
   */
  private <T> T getField(String fieldName, Tuple tuple, Class<T> clazz) {

    T value = ConversionUtils.convert(tuple.getValueByField(fieldName), clazz);
    if(value == null) {
      throw new IllegalStateException(format("Invalid tuple: missing or invalid field '%s'", fieldName));
    }

    return value;
  }

  @Override
  public BaseWindowedBolt withTumblingWindow(BaseWindowedBolt.Duration duration) {

    // need to capture the window duration for setting the flush count down
    this.windowDurationMillis = duration.value;
    return super.withTumblingWindow(duration);
  }

  public long getPeriodDurationMillis() {
    return periodDurationMillis;
  }

  public ProfileBuilderBolt withPeriodDurationMillis(long periodDurationMillis) {
    this.periodDurationMillis = periodDurationMillis;
    return this;
  }

  public ProfileBuilderBolt withPeriodDuration(int duration, TimeUnit units) {
    return withPeriodDurationMillis(units.toMillis(duration));
  }

  public ProfileBuilderBolt withProfileTimeToLiveMillis(long timeToLiveMillis) {
    this.profileTimeToLiveMillis = timeToLiveMillis;
    return this;
  }

  public long getWindowDurationMillis() {
    return windowDurationMillis;
  }

  public ProfileBuilderBolt withProfileTimeToLive(int duration, TimeUnit units) {
    return withProfileTimeToLiveMillis(units.toMillis(duration));
  }

  public ProfileBuilderBolt withEmitter(ProfileMeasurementEmitter emitter) {
    this.emitters.add(emitter);
    return this;
  }

  public DefaultMessageDistributor getMessageDistributor() {
    return messageDistributor;
  }

  public ProfileBuilderBolt withZookeeperUrl(String zookeeperUrl) {
    this.zookeeperUrl = zookeeperUrl;
    return this;
  }

  public ProfileBuilderBolt withZookeeperClient(CuratorFramework zookeeperClient) {
    this.zookeeperClient = zookeeperClient;
    return this;
  }

  public ProfileBuilderBolt withZookeeperCache(ZKCache zookeeperCache) {
    this.zookeeperCache = zookeeperCache;
    return this;
  }

  public ProfileBuilderBolt withProfilerConfigurations(ProfilerConfigurations configurations) {
    this.configurations = configurations;
    return this;
  }
  public ProfileBuilderBolt withMaxNumberOfRoutes(long maxNumberOfRoutes) {
    this.maxNumberOfRoutes = maxNumberOfRoutes;
    return this;
  }
}
