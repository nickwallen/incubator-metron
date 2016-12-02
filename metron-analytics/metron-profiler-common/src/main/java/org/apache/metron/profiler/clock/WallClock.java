package org.apache.metron.profiler.clock;

import org.json.simple.JSONObject;

/**
 * A Clock that keeps track of time using the system clock.
 */
public class WallClock implements Clock {

  @Override
  public void notify(JSONObject message) {
    // ignore; not needed
  }

  @Override
  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
