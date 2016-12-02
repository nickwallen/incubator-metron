package org.apache.metron.profiler.clock;

import org.json.simple.JSONObject;

/**
 * A Clock is responsible for managing time.  There are many
 * ways to view time.
 */
public interface Clock {

  /**
   * Notify the clock about a message that has been received.
   * @param message A telemetry message.
   */
  void notify(JSONObject message);

  /**
   * The current time in epoch milliseconds.
   */
  long currentTimeMillis();
}
