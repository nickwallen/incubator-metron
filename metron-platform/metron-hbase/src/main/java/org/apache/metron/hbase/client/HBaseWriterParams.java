package org.apache.metron.hbase.client;

import org.apache.hadoop.hbase.client.Durability;

/**
 * Parameters that define how the {@link HBaseWriter} writes to HBase.
 */
public class HBaseWriterParams {
  private Durability durability;
  private Long timeToLiveMillis;

  public HBaseWriterParams() {
    durability = Durability.USE_DEFAULT;
    timeToLiveMillis = 0L;
  }

  public HBaseWriterParams withDurability(Durability durability) {
    this.durability = durability;
    return this;
  }

  public HBaseWriterParams withTimeToLive(Long timeToLiveMillis) {
    this.timeToLiveMillis = timeToLiveMillis;
    return this;
  }

  public Durability getDurability() {
    return durability;
  }

  public Long getTimeToLiveMillis() {
    return timeToLiveMillis;
  }
}
