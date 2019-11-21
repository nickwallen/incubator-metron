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
package org.apache.metron.management;

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableMap;
import org.apache.metron.common.configuration.enrichment.SensorEnrichmentConfig;
import org.apache.metron.enrichment.adapters.geo.GeoAdapter;
import org.apache.metron.enrichment.adapters.stellar.StellarAdapter;
import org.apache.metron.enrichment.cache.CacheKey;
import org.apache.metron.enrichment.configuration.Enrichment;
import org.apache.metron.enrichment.interfaces.EnrichmentAdapter;
import org.apache.metron.enrichment.parallel.ConcurrencyContext;
import org.apache.metron.enrichment.parallel.EnrichmentStrategies;
import org.apache.metron.enrichment.parallel.EnrichmentStrategy;
import org.apache.metron.enrichment.parallel.ParallelEnricher;
import org.apache.metron.profiler.client.stellar.Util;
import org.apache.metron.stellar.dsl.BaseStellarFunction;
import org.apache.metron.stellar.dsl.Context;
import org.apache.metron.stellar.dsl.Stellar;
import org.apache.metron.stellar.dsl.StellarFunction;
import org.apache.metron.stellar.dsl.StellarFunctions;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.String.format;
import static org.apache.metron.common.configuration.ConfigurationType.ENRICHMENT;

/**
 * Stellar functions that allow the user to enrich messages in
 * the Stellar REPL.
 */
public class EnrichmentFunctions {

    /**
     * Performs the enrichment function within the Stellar REPL.
     */
    public static class Enricher {
        private SensorEnrichmentConfig enrichmentConfig;
        private ParallelEnricher enricher;
        private EnrichmentStrategies enrichmentStrategy;

        public Enricher(SensorEnrichmentConfig enrichmentConfig) {
            this.enrichmentConfig = enrichmentConfig;
            this.enrichmentStrategy = EnrichmentStrategies.ENRICHMENT;

            // only Stellar enrichments are currently supported
            Map<String, EnrichmentAdapter<CacheKey>> supportedEnrichments = new HashMap<>();
            supportedEnrichments.put("stellar", new StellarAdapter().ofType("ENRICHMENT"));

            ConcurrencyContext concurrencyContext = ConcurrencyContext.get(enrichmentStrategy);
            enricher = new ParallelEnricher(supportedEnrichments, concurrencyContext, false);
        }

        public ParallelEnricher.EnrichmentResult enrich(JSONObject message) throws ExecutionException, InterruptedException {
            return enricher.apply(message, enrichmentStrategy, enrichmentConfig, null);
        }
    }

    @Stellar(
            namespace = "ENRICHER",
            name = "INIT",
            description = "Creates an engine to execute enrichments within the REPL.",
            params = { "config - the Enrichment configuration (optional)" },
            returns = "An enrichment engine."
    )
    public static class EnricherInit extends BaseStellarFunction {

        @Override
        public Object apply(List<Object> args) {
            SensorEnrichmentConfig config = new SensorEnrichmentConfig();
            if(args.size() > 0) {
                String json = Util.getArg(0, String.class, args);
                if (json != null) {
                    config = (SensorEnrichmentConfig) ENRICHMENT.deserialize(json);
                } else {
                    throw new IllegalArgumentException(format("Invalid configuration: unable to read '%s'", json));
                }
            }
            return new Enricher(config);
        }
    }

    @Stellar(
            namespace = "ENRICHER",
            name = "ENRICH",
            description = "Performs enrichment of a message.",
            params = {
                    "enricher - The enrichment engine; see ENRICHER_INIT.",
                    "message - The message to enrich."
            },
            returns = "The enriched message."
    )
    public static class EnricherEnrich extends BaseStellarFunction {

        // TODO handle enriching a list of message(s) too

        @Override
        public Object apply(List<Object> args) {
            Enricher enricher = Util.getArg(0, Enricher.class, args);

            // parse the message that needs enriched
            JSONObject message;
            try {
                JSONParser parser = new JSONParser();
                String messageArg = Util.getArg(1, String.class, args);
                message = (JSONObject) parser.parse(messageArg);
            } catch(ParseException e) {
                throw new IllegalArgumentException("Unable to parse message", e);
            }

            // attempt to enrich the message
            try {
                ParallelEnricher.EnrichmentResult result = enricher.enrich(message);
                if(result.getEnrichmentErrors().size() > 0) {
                    Throwable error = result.getEnrichmentErrors().get(0).getValue();
                    throw new RuntimeException("Failed to enrich message.", error);
                }
                return result.getResult();

            } catch(ExecutionException | InterruptedException e) {
                throw new RuntimeException("Failed to enrich message.", e);
            }
        }
    }

}
