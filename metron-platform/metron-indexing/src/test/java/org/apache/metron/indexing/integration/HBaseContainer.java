/*
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
package org.apache.metron.indexing.integration;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.SocatContainer;
import org.testcontainers.utility.Base58;

import javax.annotation.Nonnull;
import java.util.stream.Stream;

import static org.apache.metron.indexing.integration.HBaseContainer.HBasePort.*;

public class HBaseContainer extends GenericContainer<HBaseContainer> {

  protected enum HBasePort {
    MASTER("hbase.master.port", 16000, true),
    MASTER_INFO("hbase.master.info.port", 16010, true),
    REGION_SERVER("hbase.regionserver.port", 16020, true),
    REGION_SERVER_INFO("hbase.regionserver.info.port", 16030, true),
    ZOOKEEPER("hbase.zookeeper.property.clientPort", 2181, false);

    final String name;
    final int originalPort;
    final boolean dynamic;

    HBasePort(String name, int originalPort, boolean dynamic) {
      this.name = name;
      this.originalPort = originalPort;
      this.dynamic = dynamic;
    }

    public String getName() {
      return name;
    }

    public int getOriginalPort() {
      return originalPort;
    }

    public boolean isDynamic() {
      return dynamic;
    }
  }

  protected SocatContainer proxy;

  public HBaseContainer(@Nonnull final String dockerImageName) {
    super(dockerImageName);
    withNetwork(Network.newNetwork());
    withNetworkAliases("hbase-" + Base58.randomString(6));
  }

  @Override
  protected void doStart() {
    // start the proxy container
    String networkAlias = getNetworkAliases().get(0);
    proxy = new SocatContainer()
            .withNetwork(getNetwork());

    for (HBasePort port : HBasePort.values()) {
      if (port.isDynamic()) {
        proxy.withTarget(port.getOriginalPort(), networkAlias);
      } else {
        proxy.addExposedPort(port.getOriginalPort());
      }
    }
    proxy.setWaitStrategy(null);
    proxy.start();

    for (HBasePort port : HBasePort.values()) {
      exposePortThroughProxy(networkAlias, port.getOriginalPort(), getMappedPort(port.getOriginalPort()));
    }
//    proxy = new SocatContainer()
//            .withNetwork(getNetwork())
//            .withTarget(HBasePort.MASTER.originalPort, networkAlias)
//            .withTarget(MASTER_WEB_PORT, networkAlias)
//            .withTarget(REGION_SERVER_PORT, networkAlias)
//            .withTarget(REGION_SERVER_WEB_PORT, networkAlias);
//            //.withTarget(ZOOKEEPER_PORT, networkAlias);
//    proxy.addExposedPort(ZOOKEEPER_PORT);
//    proxy.start();

//    exposePortThroughProxy(networkAlias, MASTER_PORT, proxy.getMappedPort(MASTER_PORT));
//    exposePortThroughProxy(networkAlias, MASTER_WEB_PORT, proxy.getMappedPort(MASTER_WEB_PORT));
//    exposePortThroughProxy(networkAlias, REGION_SERVER_PORT, proxy.getMappedPort(REGION_SERVER_PORT));
//    exposePortThroughProxy(networkAlias, REGION_SERVER_WEB_PORT, proxy.getMappedPort(REGION_SERVER_WEB_PORT));
//    exposePortThroughProxy(networkAlias, ZOOKEEPER_PORT, proxy.getMappedPort(ZOOKEEPER_PORT));

//    int masterPort = getMasterPort();
//    int masterWebPort = getMasterWebPort();
//    int regionServerPort = getRegionServerPort();
//    int regionServerWebPort = getRegionServerWebPort();
//    int zookeeperPort = getZookeeperPort();
//
//    System.out.println(String.format("HBase Master @ %s:%s", getMasterHostname(), masterPort));
//    System.out.println(String.format("HBase Master Info @ http://%s:%s", getMasterHostname(), masterWebPort));
//    System.out.println(String.format("HBase Region @ %s:%s", getRegionServerHostname(), regionServerPort));
//    System.out.println(String.format("HBase Region Info @ http://%s:%s", getRegionServerHostname(), regionServerWebPort));
//    System.out.println(String.format("Zookeeper @ %s:%s", getZookeeperQuorum(), zookeeperPort));

//    withExposedPorts(masterPort);
//    withExposedPorts(masterWebPort);
//    withExposedPorts(regionServerPort);
//    withExposedPorts(regionServerWebPort);
//    withExposedPorts(zookeeperPort);

//    withEnv("HBASE_MASTER_HOSTNAME", getMasterHostname());
//    withEnv("HBASE_MASTER_PORT", Integer.toString(masterPort));
//    withEnv("HBASE_MASTER_WEB_PORT", Integer.toString(masterWebPort));
//    withEnv("HBASE_REGION_SERVER_HOSTNAME", getRegionServerHostname());
//    withEnv("HBASE_REGION_SERVER_PORT", Integer.toString(regionServerPort));
//    withEnv("HBASE_REGION_SERVER_WEB_PORT", Integer.toString(regionServerWebPort));
//    withEnv("HBASE_ZOOKEEPER_QUORUM", getZookeeperQuorum());
//    withEnv("HBASE_ZOOKEEPER_PORT", Integer.toString(zookeeperPort));

//    withEnv("HBASE_MASTER_HOSTNAME", proxy.getContainerIpAddress());
//    withEnv("HBASE_MASTER_PORT", Integer.toString(MASTER_PORT));
//    withEnv("HBASE_MASTER_WEB_PORT", Integer.toString(MASTER_WEB_PORT));
//    withEnv("HBASE_REGION_SERVER_HOSTNAME", getRegionServerHostname());
//    withEnv("HBASE_REGION_SERVER_PORT", Integer.toString(REGION_SERVER_PORT));
//    withEnv("HBASE_REGION_SERVER_WEB_PORT", Integer.toString(REGION_SERVER_WEB_PORT));
//    withEnv("HBASE_ZOOKEEPER_QUORUM", getZookeeperQuorum());
//    withEnv("HBASE_ZOOKEEPER_PORT", Integer.toString(ZOOKEEPER_PORT));

    super.doStart();
  }

  @Override
  public Integer getMappedPort(int originalPort) {
    return proxy.getMappedPort(originalPort);
  }

  private void exposePortThroughProxy(String networkAlias, int originalPort, int mappedPort) {
    ExecCreateCmdResponse createCmdResponse = dockerClient
            .execCreateCmd(proxy.getContainerId())
            .withCmd("/usr/bin/socat", "TCP-LISTEN:" + originalPort + ",fork,reuseaddr", "TCP:" + networkAlias + ":" + mappedPort)
            .exec();

    dockerClient.execStartCmd(createCmdResponse.getId())
            .exec(new ExecStartResultCallback());
  }

  public String getMasterHostname() {
    return proxy.getContainerIpAddress();
  }

  public int getMasterPort() {
    return proxy.getMappedPort(MASTER.getOriginalPort());
  }

  public int getMasterWebPort() {
    return proxy.getMappedPort(MASTER_INFO.getOriginalPort());
  }

  public String getRegionServerHostname() {
    return proxy.getContainerIpAddress();
  }

  public int getRegionServerPort() {
    return proxy.getMappedPort(REGION_SERVER.getOriginalPort());
  }

  public int getRegionServerWebPort() {
    return proxy.getMappedPort(REGION_SERVER_INFO.getOriginalPort());
  }

  public String getZookeeperQuorum() {
    return proxy.getContainerIpAddress();
  }

  public int getZookeeperPort() {
    return ZOOKEEPER.getOriginalPort();
//    return proxy.getMappedPort(ZOOKEEPER.getOriginalPort());
  }

  @Override
  public void stop() {
    Stream.<Runnable>of(super::stop, proxy::stop).parallel().forEach(Runnable::run);
  }
}
