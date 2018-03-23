package org.apache.metron.profiler.bolt;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.google.code.tempusfugit.temporal.Duration.seconds;
import static com.google.code.tempusfugit.temporal.Timeout.timeout;
import static com.google.code.tempusfugit.temporal.WaitFor.waitOrTimeout;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TimerTest {

  private static final long TIMEOUT = 3;
  private Timer timer;

  @Before
  public void setup() {
    timer = new Timer();
  }

  @After
  public void tearDown() {
    timer.shutdown();
  }

  /**
   * The timer should not expire more frequently than the timer duration.  Calls to `isExpired()`
   * should not return true more frequently than the timer's duration.
   *
   * <p>In this test the calls to isExpired() occur many times, but it should not return
   * true more frequently than the timer duration.
   */
  @Test
  public void testIsExpiredWithManyCalls() throws Exception {

    // start the timer
    timer.start(250, TimeUnit.MILLISECONDS);
    long start = System.currentTimeMillis();

    // wait for the timer to return true; the timer is polled many times until it returns true
    waitOrTimeout(() -> timer.isExpired(), timeout(seconds(TIMEOUT)));
    waitOrTimeout(() -> timer.isExpired(), timeout(seconds(TIMEOUT)));
    waitOrTimeout(() -> timer.isExpired(), timeout(seconds(TIMEOUT)));
    waitOrTimeout(() -> timer.isExpired(), timeout(seconds(TIMEOUT)));
    long end = System.currentTimeMillis();

    // should have waited for the timer 4 times, 250 ms delay each time
    assertTrue(end > 0);
    double expected = 4 * 250;
    double actual = end - start;
    assertEquals(expected, actual, 100);
  }

  @Test
  public void testIsExpiredWithLongDelay() throws Exception {

    // start the timer
    timer.start(250, TimeUnit.MILLISECONDS);

    // pause the test until AFTER the timer should have expired
    Thread.sleep(500);

    // the timer should have expired by now
    assertTrue(timer.isExpired());
  }
}
