package org.apache.metron.enrichment.parallel;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.metron.enrichment.parallel.CompletableFuture9.orTimeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompletableFuture9Test {

    /**
     * Returns a value after sleeping for a fixed period of time.
     */
    private static class Sleeper implements Supplier<Integer> {
        private long sleepMillis;
        private int valueToReturn;

        public Sleeper(long sleepFor, TimeUnit units) {
            this.sleepMillis = units.toMillis(sleepFor);
            this.valueToReturn = 100;
        }

        @Override
        public Integer get() {
            try {
                Thread.sleep(sleepMillis);
            } catch (InterruptedException e) {
                // ignore
            } finally {
                return valueToReturn;
            }
        }

        public int getValueToReturn() {
            return valueToReturn;
        }
    }

    /**
     * orTimeout completes with {@link TimeoutException} if not complete
     */
    @Test
    public void testOrTimeoutWithTimeout() {
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(new Sleeper(1000, MILLISECONDS));
        CompletableFuture<Integer> timedFuture = orTimeout(future, 100, MILLISECONDS);

        // the future will sleep for 1000 millis, but we only wait for 100 millis, which will cause a timeout
        assertThrows(ExecutionException.class, () -> timedFuture.get(10, SECONDS));
    }

    /**
     * orTimeout completes normally if completed before timeout
     */
    @Test
    public void testOrTimeoutCompleted() throws InterruptedException, ExecutionException, TimeoutException {
        Sleeper sleeper = new Sleeper(1, MILLISECONDS);
        CompletableFuture<Integer> future = CompletableFuture.supplyAsync(sleeper);
        CompletableFuture<Integer> timedFuture = orTimeout(future, 100, MILLISECONDS);

        // the future will sleep for 1 millis, but we wait up to 100 millis, which should not cause a timeout
        assertEquals(sleeper.getValueToReturn(), timedFuture.get(10, SECONDS));
    }
}
