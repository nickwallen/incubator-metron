package org.apache.metron.profiler.bolt;

import org.apache.commons.lang3.concurrent.TimedSemaphore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.TimeUnit;

/**
 * A countdown timer that can be used as a non-blocking signal.
 *
 * <p>Calls to isReady() will return true no more frequently than the timer duration.  If
 * the timer duration
 */
public class Timer {

  protected static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  /**
   * The semaphore which drives the timer.
   */
  TimedSemaphore semaphore;

  /**
   * Start the timer.
   *
   * @param duration The timer duration.
   * @param durationUnits The timer duration units.
   */
  public Timer start(long duration, TimeUnit durationUnits) {
    semaphore = new TimedSemaphore(duration, durationUnits, 1);

    // acquire the semaphore to force the next caller to wait; this will not block
    acquire();

    return this;
  }

  /**
   * Returns true, if the timer count down has completed.  Otherwise, false.  This
   * call will never block.
   *
   * @return True, if the timer is ready.  False, otherwise.
   */
  public boolean isExpired() {
    boolean expired = semaphore.tryAcquire();
    LOG.debug("Is timer expired? expired={}", expired);
    return expired;
  }

  public void shutdown() {
    if(!semaphore.isShutdown()) {
      semaphore.shutdown();
    }
  }

  private void acquire() {
    try {
      semaphore.acquire();

    } catch(InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
