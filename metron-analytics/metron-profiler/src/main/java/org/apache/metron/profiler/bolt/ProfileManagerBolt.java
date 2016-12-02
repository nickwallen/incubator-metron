package org.apache.metron.profiler.bolt;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.metron.common.bolt.ConfiguredProfilerBolt;
import org.apache.metron.common.configuration.profiler.ProfileConfig;
import org.apache.metron.common.utils.ConversionUtils;
import org.apache.metron.profiler.ProfileBuilder;
import org.apache.metron.profiler.clock.Clock;
import org.apache.metron.profiler.clock.EventClock;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

/**
 * A bolt that is responsible for managing a Profile.
 *
 * A single instance of this bolt expects to see all of the messages
 * applied to a Profile.  Management functions that require visibility
 * across all messages applied to a profile, can be performed in this bolt.
 *
 * This bolt emits the same stream of data that it receives partitioned on the
 * the [profile, entity].  The bolt may inject additional messages into the
 * stream.
 */
public class ProfileManagerBolt extends ConfiguredProfilerBolt {

  protected static final Logger LOG = LoggerFactory.getLogger(ProfileManagerBolt.class);

  private OutputCollector collector;

  /**
   * If a message has not been applied to a Profile in this number of milliseconds,
   * the Profile will be forgotten and its resources will be cleaned up.
   *
   * The TTL must be at least greater than the period duration.
   */
  private long timeToLiveMillis;

  /**
   * The duration of each profile period in milliseconds.
   */
  private long periodDurationMillis;

  private long tickDurationMillis;

  /**
   * Maintains a unique clock for each profile.
   */
  private transient Cache<String, Clock> clockCache;

  private transient Cache<String, Long> lastTickCache;

  /**
   * @param zookeeperUrl The Zookeeper URL that contains the configuration data.
   */
  public ProfileManagerBolt(String zookeeperUrl) {
    super(zookeeperUrl);
  }

  /**
   * Defines the frequency at which the bolt will receive tick tuples.  Tick tuples are
   * used to control how often a profile is flushed.
   */
  @Override
  public Map<String, Object> getComponentConfiguration() {
    // TODO need to send on regular frequency??

    // how frequently should the bolt receive tick tuples?
    Config conf = new Config();
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, tickDurationMillis);
    return conf;
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("entity", "profile", "message"));
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    super.prepare(stormConf, context, collector);

    if(timeToLiveMillis < periodDurationMillis) {
      throw new IllegalStateException(format(
              "invalid configuration: expect profile TTL (%d) to be greater than period duration (%d)",
              timeToLiveMillis,
              periodDurationMillis));
    }

    this.collector = collector;
    this.clockCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(timeToLiveMillis, TimeUnit.MILLISECONDS)
            .build();
    this.lastTickCache = CacheBuilder
            .newBuilder()
            .expireAfterAccess(timeToLiveMillis, TimeUnit.MILLISECONDS)
            .build();
  }

  @Override
  public void execute(Tuple input) {
    try {
      doExecute(input);

    } catch (Throwable e) {
      LOG.error(format("Unexpected failure: '%s', tuple='%s'", e.getMessage(), input), e);
      collector.reportError(e);

    } finally {
      collector.ack(input);
    }
  }

  private Tuple createTick(long tickTimeInMillis) {


    // TODO return a tick
    return null;

  }

  public void doExecute(Tuple tuple) throws ExecutionException {

    // update the clock
    JSONObject message = getField("message", tuple, JSONObject.class);
    Clock clock = getClock(tuple);
    clock.notify(message);

    long now = clock.currentTimeMillis();
    if(now > getLastTick(tuple) + periodDurationMillis) {

      // TODO send tick tuple
      collector.emit(tick);


      // TODO update last tick cache

      // TODO use different stream for ticks?
    }
  }

  protected Clock getClock(Tuple tuple) throws ExecutionException {
    // TODO how to make this configurable to return different clocks with different params?
    return clockCache.get(cacheKey(tuple), () -> new EventClock("timestamp", 500));
  }

  protected Long getLastTick(Tuple tuple) throws ExecutionException {
    return lastTickCache.get(cacheKey(tuple), () -> new Long(0));
  }

  protected String cacheKey(Tuple tuple) {
    ProfileConfig definition = getField("profile", tuple, ProfileConfig.class);
    return definition.getProfile();
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
      throw new IllegalStateException(format("invalid tuple received: missing field '%s'", fieldName));
    }

    return value;
  }
}
