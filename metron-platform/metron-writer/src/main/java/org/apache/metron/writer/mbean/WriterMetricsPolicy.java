package org.apache.metron.writer.mbean;

import org.apache.metron.common.configuration.writer.WriterConfiguration;
import org.apache.metron.common.system.Clock;
import org.apache.metron.common.writer.BulkMessage;
import org.apache.metron.common.writer.BulkWriterResponse;
import org.apache.metron.writer.FlushPolicy;

import java.util.List;

public class WriterMetricsPolicy<MESSAGE_T> implements FlushPolicy<MESSAGE_T> {

  private WriterMetricsMBean metricsMBean;
  private Clock clock;
  private long startTime;
  private int batchSize;

  public WriterMetricsPolicy() {
    this(new Clock());
  }

  public WriterMetricsPolicy(Clock clock) {
    // does Spring have to create this for the mbean stuff to work?
    this.metricsMBean = new WriterMetricsMBean("todo");
    this.clock = clock;
  }

  @Override
  public boolean shouldFlush(String sensorType, WriterConfiguration configurations, List<BulkMessage<MESSAGE_T>> bulkMessages) {
    // nothing to do
    return false;
  }

  @Override
  public void preFlush(String sensorType, WriterConfiguration configurations, List<BulkMessage<MESSAGE_T>> bulkMessages) {
    startTime = clock.currentTimeMillis();
    batchSize = bulkMessages.size();
  }

  @Override
  public void postFlush(String sensorType, BulkWriterResponse response) {
    long endTime = clock.currentTimeMillis();
    WriterMetrics writerMetrics = new WriterMetrics()
            .withSensorType(sensorType)
            .withBatchSize(batchSize)
            .withWriteDuration(endTime - startTime);
    metricsMBean.put(writerMetrics);
  }
}
