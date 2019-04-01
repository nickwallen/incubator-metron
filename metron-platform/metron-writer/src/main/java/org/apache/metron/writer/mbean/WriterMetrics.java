package org.apache.metron.writer.mbean;

public class WriterMetrics {

  private String sensorType;
  private int batchSize;
  private long writeDuration;

  public String getSensorType() {
    return sensorType;
  }

  public void setSensorType(String sensorType) {
    this.sensorType = sensorType;
  }

  public WriterMetrics withSensorType(String sensorType) {
    this.sensorType = sensorType;
    return this;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public void setBatchSize(int batchSize) {
    this.batchSize = batchSize;
  }

  public WriterMetrics withBatchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  public long getWriteDuration() {
    return writeDuration;
  }

  public void setWriteDuration(long writeDuration) {
    this.writeDuration = writeDuration;
  }

  public WriterMetrics withWriteDuration(long writeDuration) {
    this.writeDuration = writeDuration;
    return this;
  }
}
