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
package org.apache.metron.enrichment.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import com.google.common.base.Joiner;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Sets;
import org.apache.metron.common.bolt.ConfiguredBolt;
import org.apache.metron.common.utils.ErrorUtils;
import org.json.simple.JSONObject;
import org.apache.metron.common.bolt.ConfiguredEnrichmentBolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public abstract class JoinBolt<V> extends ConfiguredEnrichmentBolt {

  private static final Logger LOG = LoggerFactory
          .getLogger(JoinBolt.class);
  protected OutputCollector collector;

  protected transient CacheLoader<String, Map<String, V>> loader;
  protected transient LoadingCache<String, Map<String, V>> cache;
  protected Long maxCacheSize;
  protected Long maxTimeRetain;

  public JoinBolt(String zookeeperUrl) {
    super(zookeeperUrl);
  }

  public JoinBolt withMaxCacheSize(long maxCacheSize) {
    this.maxCacheSize = maxCacheSize;
    return this;
  }

  public JoinBolt withMaxTimeRetain(long maxTimeRetain) {
    this.maxTimeRetain = maxTimeRetain;
    return this;
  }

  @Override
  public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
    super.prepare(map, topologyContext, outputCollector);
    this.collector = outputCollector;
    if (this.maxCacheSize == null) {
      throw new IllegalStateException("maxCacheSize must be specified");
    }
    if (this.maxTimeRetain == null) {
      throw new IllegalStateException("maxTimeRetain must be specified");
    }
    loader = new CacheLoader<String, Map<String, V>>() {
      public Map<String, V> load(String key) throws Exception {
        return new HashMap<>();
      }
    };
    cache = CacheBuilder.newBuilder().maximumSize(maxCacheSize)
            .expireAfterWrite(maxTimeRetain, TimeUnit.MINUTES)
            .build(loader);
    prepare(map, topologyContext);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void execute(Tuple tuple) {
    String streamId = tuple.getSourceStreamId();
    String key = (String) tuple.getValueByField("key");
    String subgroup = (String) tuple.getValueByField("subgroup");
    streamId = Joiner.on(":").join("" + streamId, subgroup == null?"":subgroup);
    V message = (V) tuple.getValueByField("message");
    try {
      Map<String, V> streamMessageMap = cache.get(key);
      if (streamMessageMap.containsKey(streamId)) {
        LOG.warn(String.format("Received key %s twice for " +
                "stream %s", key, streamId));
      }
      streamMessageMap.put(streamId, message);
      Set<String> streamIds = getStreamIds(message);
      Set<String> streamMessageKeys = streamMessageMap.keySet();
      if ( streamMessageKeys.size() == streamIds.size()
        && Sets.symmetricDifference(streamMessageKeys, streamIds)
               .isEmpty()
         ) {
        collector.emit( "message"
                      , tuple
                      , new Values( key
                                  , joinMessages(streamMessageMap)
                                  )
                      );
        cache.invalidate(key);
        collector.ack(tuple);
        LOG.trace("Emitted message for key: {}", key);
      } else {
        cache.put(key, streamMessageMap);
        if(LOG.isDebugEnabled()) {
          LOG.debug(getClass().getSimpleName() + ": Missed joining portions for "+ key + ". Expected " + Joiner.on(",").join(streamIds)
                  + " != " + Joiner.on(",").join(streamMessageKeys)
                   );
        }
      }
    } catch (Exception e) {
      LOG.error("[Metron] Unable to join messages: " + message, e);
      JSONObject error = ErrorUtils.generateErrorMessage("Joining problem: " + message, e);
      collector.ack(tuple);
      collector.emit("error", new Values(error));
      collector.reportError(e);
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("message", new Fields("key", "message"));
    declarer.declareStream("error", new Fields("message"));
  }

  public abstract void prepare(Map map, TopologyContext topologyContext);

  public abstract Set<String> getStreamIds(V value);

  public abstract V joinMessages(Map<String, V> streamMessageMap);
}
