package org.apache.metron.enrichment.lookup;

import org.apache.hadoop.hbase.client.Table;
import org.apache.metron.enrichment.converter.EnrichmentKey;
import org.apache.metron.enrichment.converter.EnrichmentValue;
import org.apache.metron.enrichment.lookup.handler.HBaseContext;
import org.apache.metron.enrichment.lookup.handler.KeyWithContext;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockEnrichmentLookup extends EnrichmentLookup {

  private Map<EnrichmentKey, EnrichmentValue> enrichments;

  public MockEnrichmentLookup() {
    this.enrichments = new HashMap<>();
  }

  public MockEnrichmentLookup withEnrichment(EnrichmentKey key, EnrichmentValue value) {
    this.enrichments.put(key, value);
    return this;
  }

  @Override
  public Iterable<LookupKV<EnrichmentKey, EnrichmentValue>> get(
          Iterable<KeyWithContext<EnrichmentKey, HBaseContext>> keyWithContexts,
          boolean logAccess) throws IOException {
    List<LookupKV<EnrichmentKey, EnrichmentValue>> results = new ArrayList<>();

    for(KeyWithContext<EnrichmentKey, HBaseContext> key: keyWithContexts) {
      if(enrichments.containsKey(key.getKey())) {
        EnrichmentKey enrichmentKey = key.getKey();
        EnrichmentValue enrichmentValue = enrichments.get(enrichmentKey);
        results.add(new LookupKV<>(enrichmentKey, enrichmentValue));
      }
    }

    return results;
  }

  @Override
  public Table getTable() {
    return Mockito.mock(Table.class);
  }
}
