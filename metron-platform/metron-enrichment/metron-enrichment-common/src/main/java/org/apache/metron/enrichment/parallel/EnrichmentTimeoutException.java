package org.apache.metron.enrichment.parallel;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The exception thrown when enrichment exceeds the timeout.
 */
public class EnrichmentTimeoutException extends TimeoutException {

    public EnrichmentTimeoutException(long timeout, TimeUnit timeoutUnits) {
        super(String.format("%s %s enrichment timeout exceeded", timeout, timeoutUnits.toString().toLowerCase()));
    }
}
