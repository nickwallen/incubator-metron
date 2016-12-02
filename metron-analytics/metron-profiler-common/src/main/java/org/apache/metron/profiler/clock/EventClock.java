package org.apache.metron.profiler.clock;

import org.apache.metron.common.utils.ConversionUtils;
import org.json.simple.JSONObject;

/**
 * A clock that keeps track of time by monitoring a timestamp field
 * contained within the stream of telemetry data.
 */
public class EventClock implements Clock {

  /**
   * The name of the field that contains a timestamp in the form
   * of epoch milliseconds.
   */
  private String timestampField;

  /**
   * The last known timestamp in epoch milliseconds.
   */
  private long lastKnownTimestamp;

  /**
   * This clock assumes that the data is processed in a manner that is
   * fairly close to monotonically increasing.  The offset is deducted from
   * the last known timestamp to provide some flexibility in handling
   * out-of-order messages.
   */
  private long offset;

  /**
   * @param timestampField The name of the field containing a timestamp.
   * @param offset The offset used when determining current time.
   */
  public EventClock(String timestampField, long offset) {
    this.timestampField = timestampField;
    this.offset = offset;
    this.lastKnownTimestamp = 0;
  }

  @Override
  public void notify(JSONObject message) {
    Object value = message.get(timestampField);
    if(value != null) {
      lastKnownTimestamp = Math.max(lastKnownTimestamp, ConversionUtils.convert(value, Long.class));
    }
  }

  @Override
  public long currentTimeMillis() {
    return Math.max(lastKnownTimestamp - offset, 0);
  }
}
