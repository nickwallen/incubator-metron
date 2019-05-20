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

package org.apache.metron.enrichment.adapters.simplehbase;


import com.google.common.collect.Iterables;
import org.apache.metron.common.configuration.enrichment.EnrichmentConfig;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.lookup.EnrichmentLookup;
import org.apache.metron.enrichment.lookup.LookupKV;
import org.apache.metron.enrichment.lookup.accesstracker.NoopAccessTracker;
import org.apache.metron.enrichment.lookup.handler.HBaseContext;
import org.apache.metron.enrichment.lookup.handler.KeyWithContext;
import org.apache.metron.enrichment.utils.EnrichmentUtils;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

public class SimpleHBaseAdapter implements EnrichmentAdapter<CacheKey>,Serializable {
  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private SimpleHBaseConfig config;
  private EnrichmentLookup lookup;

  public SimpleHBaseAdapter() {
  }

  public SimpleHBaseAdapter(SimpleHBaseConfig config) {
    withConfig(config);
  }

  public SimpleHBaseAdapter withEnrichmentLookup(EnrichmentLookup lookup) {
    this.lookup = lookup;
    return this;
  }

  public SimpleHBaseAdapter withConfig(SimpleHBaseConfig config) {
    this.config = config;
    return this;
  }

  @Override
  public void logAccess(CacheKey value) {
  }

  public boolean isInitialized() {
    return lookup != null && lookup.getTable() != null;
  }

  @Override
  public JSONObject enrich(CacheKey value) {
    JSONObject enriched = new JSONObject();
    if(!isInitialized()) {
      initializeAdapter(null);
    }
    List<String> enrichmentTypes = value.getConfig()
                                        .getEnrichment().getFieldToTypeMap()
                                        .get(EnrichmentUtils.toTopLevelField(value.getField()));
    if(isInitialized() && enrichmentTypes != null && value.getValue() != null) {
      try {
        String valueAsString = value.coerceValue(String.class);
        EnrichmentConfig config = value.getConfig().getEnrichment();
        EnrichmentUtils.TypeToKey typeToKey = new EnrichmentUtils.TypeToKey(valueAsString, lookup.getTable(), config);
        Iterable<KeyWithContext<EnrichmentKey, HBaseContext>> keysWithContext = Iterables.transform(enrichmentTypes, typeToKey);
        for (LookupKV<EnrichmentKey, EnrichmentValue> kv: lookup.get(keysWithContext, false)) {
          if (kv != null && kv.getValue() != null && kv.getValue().getMetadata() != null) {
            EnrichmentKey enrichmentKey = kv.getKey();
            EnrichmentValue enrichmentValue = kv.getValue();
            for (Map.Entry<String, Object> values : enrichmentValue.getMetadata().entrySet()) {
              enriched.put(enrichmentKey.type + "." + values.getKey(), values.getValue());
            }
            LOG.trace("Enriched type {} => {}", enrichmentKey.type, enriched);
          }
        }
      } catch (IOException e) {
        LOG.error("Unable to retrieve value: {}", e.getMessage(), e);
        initializeAdapter(null);
        throw new RuntimeException("Unable to retrieve value: " + e.getMessage(), e);
      }
    }
    LOG.trace("SimpleHBaseAdapter succeeded: {}", enriched);
    return enriched;
  }

  @Override
  public boolean initializeAdapter(Map<String, Object> configuration) {
    if(lookup == null) {
      try {
        lookup = new EnrichmentLookup(
                config.getConnectionFactory(),
                config.getHBaseTable(),
                config.getHBaseCF(),
                new NoopAccessTracker());

      } catch (IOException e) {
        LOG.error("Unable to initialize adapter: {}", e.getMessage(), e);
        return false;
      }
    }
    return true;
  }

  @Override
  public void updateAdapter(Map<String, Object> config) {
  }

  @Override
  public void cleanup() {
    try {
      lookup.close();
    } catch (Exception e) {
      LOG.error("Unable to cleanup access tracker", e);
    }
  }

  @Override
  public String getOutputPrefix(CacheKey value) {
    return value.getField();
  }
}
