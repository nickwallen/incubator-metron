package org.apache.metron.enrichment.lookup;

import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs a lookup for enrichment values using a collection stored in memory.
 */
public class InMemoryEnrichmentLookup implements EnrichmentLookup {
  private Map<EnrichmentKey, EnrichmentValue> enrichments;

  public InMemoryEnrichmentLookup() {
    this.enrichments = new HashMap<>();
  }

  /**
   * Add an enrichment.
   * @param key The enrichment key.
   * @param value The enrichment value.
   * @return
   */
  public InMemoryEnrichmentLookup withEnrichment(EnrichmentKey key, EnrichmentValue value) {
    this.enrichments.put(key, value);
    return this;
  }

  @Override
  public boolean isInitialized() {
    return true;
  }

  @Override
  public boolean exists(EnrichmentKey key) throws IOException {
    return enrichments.containsKey(key);
  }

  @Override
  public Iterable<Boolean> exists(Iterable<EnrichmentKey> keys) throws IOException {
    List<Boolean> results = new ArrayList<>();
    for(EnrichmentKey key: keys) {
      results.add(enrichments.containsKey(key));
    }
    return results;
  }

  @Override
  public EnrichmentResult get(EnrichmentKey key) throws IOException {
    EnrichmentValue enrichmentValue = enrichments.get(key);
    return new EnrichmentResult(key, enrichmentValue);
  }

  @Override
  public Iterable<EnrichmentResult> get(Iterable<EnrichmentKey> keys) throws IOException {
    List<EnrichmentResult> results = new ArrayList<>();
    for(EnrichmentKey key: keys) {
      if(enrichments.containsKey(key)) {
        results.add(get(key));
      }
    }

    return results;
  }

  @Override
  public void close() throws IOException {
    // nothing to do
  }

}
