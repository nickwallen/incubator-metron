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
package org.apache.metron.enrichment.parallel;

import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.metron.common.Constants;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.common.configuration.enrichment.handler.ConfigHandler;
import org.apache.metron.common.performance.PerformanceLogger;
import org.apache.metron.common.utils.MessageUtils;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.utils.EnrichmentUtils;
import org.apache.metron.stellar.common.utils.ConversionUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * This is an independent component which will accept a message and a set of enrichment adapters as well as a config which defines
 * how those enrichments should be performed and fully enrich the message.  The result will be the enriched message
 * unified together and a list of errors which happened.
 */
public class ParallelEnricher {
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final String ENRICHMENT_MESSAGE_TIMEOUT = "message.timeout.millis";
  private static final String ENRICHMENT_BLOCK_TIMEOUT = "block.timeout.millis";
  private Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType = new HashMap<>();
  private EnumMap<EnrichmentStrategies, CacheStats> cacheStats = new EnumMap<>(EnrichmentStrategies.class);

  /**
   * The result of an enrichment.
   */
  public static class EnrichmentResult {
    private JSONObject result;
    private List<Map.Entry<Object, Throwable>> enrichmentErrors;

    public EnrichmentResult(JSONObject result, List<Map.Entry<Object, Throwable>> enrichmentErrors) {
      this.result = result;
      this.enrichmentErrors = enrichmentErrors;
    }

    /**
     * The unified fully enriched result.
     * @return
     */
    public JSONObject getResult() {
      return result;
    }

    /**
     * The errors that happened in the course of enriching.
     * @return
     */
    public List<Map.Entry<Object, Throwable>> getEnrichmentErrors() {
      return enrichmentErrors;
    }
  }

  private ConcurrencyContext concurrencyContext;

  /**
   * Construct a parallel enricher with a set of enrichment adapters associated with their enrichment types.
   * @param enrichmentsByType
   */
  public ParallelEnricher( Map<String, EnrichmentAdapter<CacheKey>> enrichmentsByType
                         , ConcurrencyContext concurrencyContext
                         , boolean logStats
                         )
  {
    this.enrichmentsByType = enrichmentsByType;
    this.concurrencyContext = concurrencyContext;
    if(logStats) {
      for(EnrichmentStrategies s : EnrichmentStrategies.values()) {
        cacheStats.put(s, null);
      }
    }
  }

  /**
   * Fully enriches a message.  Each enrichment is done in parallel via a threadpool.
   * Each enrichment is fronted with a LRU cache.
   *
   * @param message the message to enrich
   * @param strategy The enrichment strategy to use (e.g. enrichment or threat intel)
   * @param config The sensor enrichment config
   * @param perfLog The performance logger.  We log the performance for this call, the split portion and the enrichment portion.
   * @return the enrichment result
   */
  public EnrichmentResult apply( JSONObject message
                         , EnrichmentStrategies strategy
                         , SensorEnrichmentConfig config
                         , PerformanceLogger perfLog
                         ) throws ExecutionException, InterruptedException {
    if(message == null) {
      return null;
    }
    if(perfLog != null) {
      perfLog.mark("execute");
      if(perfLog.isDebugEnabled() && !cacheStats.isEmpty()) {
        CacheStats before =  cacheStats.get(strategy);
        CacheStats after = concurrencyContext.getCache().stats();
        if(before != null && after != null) {
          CacheStats delta = after.minus(before);
          perfLog.log("cache", delta.toString());
        }
        cacheStats.put(strategy, after);
      }
    }
    String sensorType = MessageUtils.getSensorType(message);
    message.put(getClass().getSimpleName().toLowerCase() + ".splitter.begin.ts", "" + System.currentTimeMillis());
    // Split the message into individual tasks.
    //
    // A task will either correspond to an enrichment adapter or,
    // in the case of Stellar, a stellar subgroup.  The tasks will be grouped by enrichment type (the key of the
    //tasks map).  Each JSONObject will correspond to a unit of work.
    Map<String, List<JSONObject>> tasks = splitMessage( message
                                                      , strategy
                                                      , config
                                                      );
    message.put(getClass().getSimpleName().toLowerCase() + ".splitter.end.ts", "" + System.currentTimeMillis());
    message.put(getClass().getSimpleName().toLowerCase() + ".enrich.begin.ts", "" + System.currentTimeMillis());
    if(perfLog != null) {
      perfLog.mark("enrich");
    }
    List<CompletableFuture<JSONObject>> taskList = new ArrayList<>();
    List<Map.Entry<Object, Throwable>> errors = Collections.synchronizedList(new ArrayList<>());
    for(Map.Entry<String, List<JSONObject>> task : tasks.entrySet()) {
      //task is the list of enrichment tasks for the task.getKey() adapter
      EnrichmentAdapter<CacheKey> adapter = enrichmentsByType.get(task.getKey());
      if(adapter == null) {
        throw new IllegalStateException("Unable to find an adapter for " + task.getKey()
                + ", possible adapters are: " + Joiner.on(",").join(enrichmentsByType.keySet()));
      }
      message.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".begin.ts", "" + System.currentTimeMillis());
      final Optional<Long> blockTimeout = getEnrichmentBlockTimeout(config);
      for(JSONObject m : task.getValue()) {
        /* now for each unit of work (each of these only has one element in them)
         * the key is the field name and the value is value associated with that field.
         *
         * In the case of stellar enrichment, the field name is the subgroup name or empty string.
         * The value is the subset of the message needed for the enrichment.
         *
         * In the case of another enrichment (e.g. hbase), the field name is the field name being enriched.
         * The value is the corresponding value.
         */
        for(Object o : m.keySet()) {
          String field = (String) o;
          Object value = m.get(o);
          if(value == null) {
            message.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".end.ts", "" + System.currentTimeMillis());
            continue;
          }
          CacheKey cacheKey = new CacheKey(field, value, config);
          String prefix = adapter.getOutputPrefix(cacheKey);
          Supplier<JSONObject> supplier = () -> {
            try {
              JSONObject ret = concurrencyContext.getCache().get(cacheKey, new EnrichmentCallable(cacheKey, adapter));
              if(ret == null) {
                ret = new JSONObject();
              }
              //each enrichment has their own unique prefix to use to adjust the keys for the enriched fields.
              JSONObject adjustedKeys = EnrichmentUtils
                  .adjustKeys(new JSONObject(), ret, cacheKey.getField(), prefix);
              adjustedKeys.put("adapter." + adapter.getClass().getSimpleName().toLowerCase() + ".end.ts", "" + System.currentTimeMillis());
              return adjustedKeys;
            } catch (Throwable e) {
              errors.add(createError(strategy, sensorType, task.getKey(), m, e));
              return new JSONObject();
            }
          };

          CompletableFuture<JSONObject> future = CompletableFuture.supplyAsync(supplier, ConcurrencyContext.getExecutor());
          if(blockTimeout.isPresent() && blockTimeout.get() > 0L) {
            // ensure the enrichment 'block' takes no longer than the timeout
            future = withTimeout(future, blockTimeout.get());

            // if the block exceeds the timeout, create an enrichment error
            future = future.exceptionally(e -> {
              LOG.debug("Enrichments in '{}' failed to complete within {} millisecond(s); guid={}", field, blockTimeout.get(), message.get(Constants.GUID));
              errors.add(createError(strategy, sensorType, task.getKey(), m, e));
              return new JSONObject();
            });
          }

          //add the Future to the task list
          taskList.add(future);
        }
      }
    }
    if(taskList.isEmpty()) {
      message.put(getClass().getSimpleName().toLowerCase() + ".enrich.end.ts", "" + System.currentTimeMillis());
      return new EnrichmentResult(message, errors);
    }

    CompletableFuture<JSONObject> enrichments = all(taskList, message, (left, right) -> join(left, right));
    JSONObject enrichedMessage = new JSONObject();
    Optional<Long> messageTimeout = getEnrichmentMessageTimeout(config);
    try {
      if(messageTimeout.isPresent()) {
        // enforce the enrichment message timeout
        enrichedMessage = enrichments.get(messageTimeout.get(), TimeUnit.MILLISECONDS);
      } else {
        // no enrichment message timeout
        enrichedMessage = enrichments.get();
      }

    } catch(TimeoutException e) {
      LOG.debug("Enrichment failed to complete within timeout; guid={}, {}={} millis",
              message.get(Constants.GUID), ENRICHMENT_MESSAGE_TIMEOUT, messageTimeout.get());
      enrichedMessage = message;
      errors.add(createError(sensorType, enrichedMessage, e));

    } finally {
      enrichedMessage.put(getClass().getSimpleName().toLowerCase() + ".enrich.end.ts", "" + System.currentTimeMillis());
    }

    EnrichmentResult ret = new EnrichmentResult(enrichedMessage, errors);
    if(perfLog != null) {
      String key = message.get(Constants.GUID) + "";
      perfLog.log("enrich", "key={}, elapsed time to enrich", key);
      perfLog.log("execute", "key={}, elapsed time to run execute", key);
    }
    return ret;
  }

  private Optional<Long> getEnrichmentMessageTimeout(SensorEnrichmentConfig enrichmentConfig) {
    return getConfigurationValue(enrichmentConfig, ENRICHMENT_MESSAGE_TIMEOUT);
  }

  private Optional<Long> getEnrichmentBlockTimeout(SensorEnrichmentConfig enrichmentConfig) {
    return getConfigurationValue(enrichmentConfig, ENRICHMENT_BLOCK_TIMEOUT);
  }

  private Optional<Long> getConfigurationValue(SensorEnrichmentConfig enrichmentConfig, String key) {
    Optional<Long> result = Optional.empty();
    Map<String, Object> config = enrichmentConfig.getEnrichment().getConfig();
    if(config.containsKey(key)) {
      long timeout = ConversionUtils.convert(config.get(key), Long.class);
      if(timeout > 0) {
        result = Optional.of(timeout);
      }
    }

    return result;
  }

  private static Map.Entry<Object, Throwable> createError(EnrichmentStrategies strategy, String sensorType, String enrichmentAdapter, JSONObject m, Throwable e) {
    JSONObject errorMessage = new JSONObject();
    errorMessage.putAll(m);
    errorMessage.put(Constants.SENSOR_TYPE, sensorType );
    Exception exception = new IllegalStateException(strategy + " error with " + enrichmentAdapter + " failed: " + e.getMessage(), e);
    return new AbstractMap.SimpleEntry<>(errorMessage, exception);
  }

  private static Map.Entry<Object, Throwable> createError(String sensorType, JSONObject m, Throwable e) {
    JSONObject errorMessage = new JSONObject();
    errorMessage.putAll(m);
    errorMessage.put(Constants.SENSOR_TYPE, sensorType );
    return new AbstractMap.SimpleEntry<>(errorMessage, e);
  }

  /**
   * Creates a future that will throw an exception if computation of the original future
   * takes longer than the timeout.
   *
   * http://iteratrlearning.com/java9/2016/09/13/java9-timeouts-completablefutures.html
   * https://www.nurkiewicz.com/2014/12/asynchronous-timeouts-with.html
   *
   * @param future The original future.
   * @param timeoutMillis The maximum time to wait for the original future to compute.
   * @return A {@link CompletableFuture} that will execute no longer than the timeout.
   */
  @SuppressWarnings("unchecked")
  private static <T> CompletableFuture<T> withTimeout(CompletableFuture<T> future, long timeoutMillis) {
    return (CompletableFuture<T>) CompletableFuture.anyOf(future, timeoutAfter(timeoutMillis, TimeUnit.MILLISECONDS));
  }

  /**
   * Creates a {@link CompletableFuture} that will throw an exception after a fixed
   * period of time.
   * @param timeout The timeout value.
   * @param timeoutUnits The units of the timeout value.
   * @return A {@link CompletableFuture}.
   */
  private static <T> CompletableFuture<T> timeoutAfter(long timeout, TimeUnit timeoutUnits) {
    CompletableFuture<T> result = new CompletableFuture<>();
    timeoutScheduler.schedule(
            () -> result.completeExceptionally(new EnrichmentTimeoutException(timeout, timeoutUnits)),
            timeout,
            timeoutUnits);
    return result;
  }

  private static final ScheduledExecutorService timeoutScheduler = Executors.newScheduledThreadPool(1,
          new ThreadFactoryBuilder()
                  .setNameFormat("enrichment-timeout-%d")
                  .setDaemon(true)
                  .build());

  private static JSONObject join(JSONObject left, JSONObject right) {
    JSONObject message = new JSONObject();
    message.putAll(left);
    message.putAll(right);
    List<Object> emptyKeys = new ArrayList<>();
    for(Object key : message.keySet()) {
      Object value = message.get(key);
      if(value == null || value.toString().length() == 0) {
        emptyKeys.add(key);
      }
    }
    for(Object o : emptyKeys) {
      message.remove(o);
    }
    return message;
  }

  /**
   * Wait until all the futures complete and join the resulting JSONObjects using the supplied binary operator
   * and identity object.
   *
   * @param futures
   * @param identity
   * @param reduceOp
   * @return
   */
  public static CompletableFuture<JSONObject> all(
            List<CompletableFuture<JSONObject>> futures
          , JSONObject identity
          , BinaryOperator<JSONObject> reduceOp
  ) {
    CompletableFuture[] cfs = futures.toArray(new CompletableFuture[futures.size()]);
    CompletableFuture<Void> future = CompletableFuture.allOf(cfs);
    return future.thenApply(aVoid -> futures.stream().map(CompletableFuture::join).reduce(identity, reduceOp));
  }

  /**
   * Take a message and a config and return a list of tasks indexed by adapter enrichment types.
   * @param message
   * @param enrichmentStrategy
   * @param config
   * @return
   */
  public Map<String, List<JSONObject>> splitMessage( JSONObject message
                                                   , EnrichmentStrategy enrichmentStrategy
                                                   , SensorEnrichmentConfig config
                                                   ) {
    Map<String, List<JSONObject>> streamMessageMap = new HashMap<>();
    Map<String, Object> enrichmentFieldMap = enrichmentStrategy.getUnderlyingConfig(config).getFieldMap();

    Map<String, ConfigHandler> fieldToHandler = enrichmentStrategy.getUnderlyingConfig(config).getEnrichmentConfigs();

    Set<String> enrichmentTypes = new HashSet<>(enrichmentFieldMap.keySet());

    //the set of enrichments configured
    enrichmentTypes.addAll(fieldToHandler.keySet());

    //For each of these enrichment types, we're going to construct JSONObjects
    //which represent the individual enrichment tasks.
    for (String enrichmentType : enrichmentTypes) {
      Object fields = enrichmentFieldMap.get(enrichmentType);
      ConfigHandler retriever = fieldToHandler.get(enrichmentType);

      //How this is split depends on the ConfigHandler
      List<JSONObject> enrichmentObject = retriever.getType()
              .splitByFields( message
                      , fields
                      , field -> enrichmentStrategy.fieldToEnrichmentKey(enrichmentType, field)
                      , retriever
              );
      streamMessageMap.put(enrichmentType, enrichmentObject);
    }
    return streamMessageMap;
  }

}
