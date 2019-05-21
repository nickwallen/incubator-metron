package org.apache.metron.enrichment.lookup;

import org.apache.metron.enrichment.converter.EnrichmentKey;

import java.io.IOException;

/**
 * Performs a lookup to find the value of an enrichment.
 */
public interface EnrichmentLookup {

  /**
   * @return True, if initialization has been completed.  Otherwise, false.
   */
  boolean isInitialized();

  /**
   * Does an enrichment exist for the given key?
   *
   * @param key The enrichment key.
   * @return True, if an enrichment exists. Otherwise, false.
   * @throws IOException
   */
  boolean exists(EnrichmentKey key) throws IOException;

  /**
   * Does an enrichment exist for the given keys?
   *
   * @param keys The enrichment keys.
   * @return True, if an enrichment exists. Otherwise, false.
   * @throws IOException
   */
  Iterable<Boolean> exists(Iterable<EnrichmentKey> keys) throws IOException;

  /**
   * Retrieve the value of an enrichment.
   *
   * @param key The enrichment key.
   * @return The value of the enrichment.
   * @throws IOException
   */
  EnrichmentResult get(EnrichmentKey key) throws IOException;

  /**
   * Retrieves the value of multiple enrichments.
   *
   * @param keys The enrichment keys.
   * @return The value of the enrichments.
   * @throws IOException
   */
  Iterable<EnrichmentResult> get(Iterable<EnrichmentKey> keys) throws IOException;

  /**
   * Close and clean-up resources.
   */
  void close() throws IOException;
}
