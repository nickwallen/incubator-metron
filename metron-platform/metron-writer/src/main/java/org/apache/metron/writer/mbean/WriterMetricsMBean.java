package org.apache.metron.writer.mbean;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.lang.invoke.MethodHandles;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class WriterMetricsMBean {

  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private String writerName;
  private Map<String, WriterMetrics> metricsBySensor;

  public WriterMetricsMBean(String writerName) {
    this.writerName = writerName;
    this.metricsBySensor = new HashMap<>();
  }

  public void register() {
    // name the mBean
    ObjectName mBeanName = null;
    try {
      mBeanName = new ObjectName("org.apache.metron.writer:type=basic,name=writer");
    } catch (MalformedObjectNameException e) {
      LOG.error("Unable to reserve MBean name; name={}", mBeanName, e);
      throw new RuntimeException(e);
    }

    // register the mbean
    MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    try {
      server.registerMBean(this, mBeanName);
    } catch (InstanceAlreadyExistsException | MBeanRegistrationException | NotCompliantMBeanException e) {
      LOG.error("Unable to register MBean; name={}", mBeanName, e);
      throw new RuntimeException(e);
    }
  }

  public String getWriterName() {
    return writerName;
  }

  public WriterMetrics getMetricsBySensor(String sensor) {
    return metricsBySensor.get(sensor);
  }

  public Set<String> getSensors() {
    return metricsBySensor.keySet();
  }

  public void put(WriterMetrics metrics) {
    metricsBySensor.put(metrics.getSensorType(), metrics);
  }

  public void setWriterName(String writerName) {
    this.writerName = writerName;
  }

}
