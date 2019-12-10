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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;

/**
 * Provides additional functionality for {@link java.util.concurrent.CompletableFuture} that
 * is not available until Java 9.
 */
public class CompletableFuture9 {

    /**
     * Action to completeExceptionally on timeout
     */
    static final class Timeout implements Runnable {
        final CompletableFuture<?> f;

        Timeout(CompletableFuture<?> f) {
            this.f = f;
        }

        public void run() {
            if (f != null && !f.isDone()) {
                f.completeExceptionally(new TimeoutException());
            }
        }
    }

    /**
     * Action to cancel unneeded timeouts
     */
    static final class Canceller implements BiConsumer<Object, Throwable> {
        final Future<?> f;

        Canceller(Future<?> f) {
            this.f = f;
        }

        public void accept(Object ignore, Throwable ex) {
            if (ex == null && f != null && !f.isDone()) {
                f.cancel(false);
            }
        }
    }

    /**
     * Singleton delay scheduler, used only for starting and
     * cancelling tasks.
     */
    static final class Delayer {
        static ScheduledFuture<?> delay(Runnable command, long delay, TimeUnit unit) {
            return delayer.schedule(command, delay, unit);
        }

        static final class DaemonThreadFactory implements ThreadFactory {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setName("CompletableFutureDelayScheduler");
                return t;
            }
        }

        static final ScheduledThreadPoolExecutor delayer;
        static {
            delayer = new ScheduledThreadPoolExecutor(1, new DaemonThreadFactory());
        }
    }

    /**
     * Exceptionally completes a {@link CompletableFuture} with a {@link TimeoutException}
     * if not otherwise completed before the given timeout.
     *
     * This implementation mimics CompletableFuture#orTimeout that is available in Java 9. Once the project
     * migrates to Java 9, this code will not longer be necessary.
     *
     * @param future The future to complete
     * @param timeout How long to wait before completing exceptionally with a TimeoutException, in units of {@code unit}
     * @param timeoutUnits A {@code TimeUnit} determining how to interpret the {@code timeout} parameter.
     * @return A {@link CompletableFuture}.
     */
    public static <T> CompletableFuture<T> orTimeout(CompletableFuture<T> future, long timeout, TimeUnit timeoutUnits) {
        if (timeoutUnits == null) {
            throw new IllegalArgumentException("Missing timeout units");
        }
        return future.whenComplete(new Canceller(Delayer.delay(new Timeout(future), timeout, timeoutUnits)));
    }
}
