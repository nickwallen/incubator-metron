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

public class HBaseContainer extends GenericContainer<HBaseContainer> {

  private static final int MASTER_PORT = 16000;
  private static final int MASTER_INFO_PORT = 16010;
  private static final int REGION_SERVER_PORT = 16020;
  private static final int REGION_SERVER_INFO_PORT = 16030;
  private static final int ZOOKEEPER_PORT = 2181;

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
            .withNetwork(getNetwork())
            .withTarget(MASTER_PORT, networkAlias)
            .withTarget(MASTER_INFO_PORT, networkAlias)
            .withTarget(REGION_SERVER_PORT, networkAlias)
            .withTarget(REGION_SERVER_INFO_PORT, networkAlias)
            .withTarget(ZOOKEEPER_PORT, networkAlias);
    proxy.start();

    exposePortThroughProxy(networkAlias, getMasterPort());
    exposePortThroughProxy(networkAlias, getMasterInfoPort());
    exposePortThroughProxy(networkAlias, getRegionServerPort());
    exposePortThroughProxy(networkAlias, getRegionServerInfoPort());
    exposePortThroughProxy(networkAlias, getZookeeperPort());
//
//    proxy.addExposedPort(getZookeeperPort());
//    proxy.addExposedPort(getMasterPort());
//    proxy.addExposedPort(getMasterInfoPort());
//    proxy.addExposedPort(getRegionServerPort());
//    proxy.addExposedPort(getRegionServerInfoPort());

//    exposePortThroughProxy(networkAlias, MASTER_PORT, proxy.getMappedPort(MASTER_PORT));
//    exposePortThroughProxy(networkAlias, MASTER_WEB_PORT, proxy.getMappedPort(MASTER_WEB_PORT));
//    exposePortThroughProxy(networkAlias, REGION_SERVER_PORT, proxy.getMappedPort(REGION_SERVER_PORT));
//    exposePortThroughProxy(networkAlias, REGION_SERVER_WEB_PORT, proxy.getMappedPort(REGION_SERVER_WEB_PORT));
//    exposePortThroughProxy(networkAlias, ZOOKEEPER_PORT, proxy.getMappedPort(ZOOKEEPER_PORT));

    int masterPort = getMasterPort();
    int masterWebPort = getMasterInfoPort();
    int regionServerPort = getRegionServerPort();
    int regionServerWebPort = getRegionServerInfoPort();
    int zookeeperPort = getZookeeperPort();

    System.out.println(String.format("HBaseContainer: %s", proxy));
    System.out.println(String.format("HBaseContainer: HBase Master @ %s:%s", getMasterHostname(), masterPort));
    System.out.println(String.format("HBaseContainer: HBase Master Info @ http://%s:%s", getMasterHostname(), masterWebPort));
    System.out.println(String.format("HBaseContainer: HBase Region @ %s:%s", getRegionServerHostname(), regionServerPort));
    System.out.println(String.format("HBaseContainer: HBase Region Info @ http://%s:%s", getRegionServerHostname(), regionServerWebPort));
    System.out.println(String.format("HBaseContainer: Zookeeper @ %s:%s", getZookeeperQuorum(), zookeeperPort));

//    withExposedPorts(masterPort);
//    withExposedPorts(masterWebPort);
//    withExposedPorts(regionServerPort);
//    withExposedPorts(regionServerWebPort);
//    withExposedPorts(zookeeperPort);

    withEnv("HBASE_MASTER_HOSTNAME", getMasterHostname());
    withEnv("HBASE_MASTER_PORT", Integer.toString(masterPort));
    withEnv("HBASE_MASTER_WEB_PORT", Integer.toString(masterWebPort));
    withEnv("HBASE_REGION_SERVER_HOSTNAME", getRegionServerHostname());
    withEnv("HBASE_REGION_SERVER_PORT", Integer.toString(regionServerPort));
    withEnv("HBASE_REGION_SERVER_WEB_PORT", Integer.toString(regionServerWebPort));
    withEnv("HBASE_ZOOKEEPER_QUORUM", getZookeeperQuorum());
    withEnv("HBASE_ZOOKEEPER_PORT", Integer.toString(zookeeperPort));

    super.doStart();
  }

  @Override
  public Integer getMappedPort(int originalPort) {
    return proxy.getMappedPort(originalPort);
  }

  private void exposePortThroughProxy(String networkAlias, int port) {
    exposePortThroughProxy(networkAlias, port, port);
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
    return proxy.getMappedPort(MASTER_PORT);
  }

  public int getMasterInfoPort() {
    return proxy.getMappedPort(MASTER_INFO_PORT);
  }

  public String getRegionServerHostname() {
    return proxy.getContainerIpAddress();
  }

  public int getRegionServerPort() {
    return proxy.getMappedPort(REGION_SERVER_PORT);
  }

  public int getRegionServerInfoPort() {
    return proxy.getMappedPort(REGION_SERVER_INFO_PORT);
  }

  public String getZookeeperQuorum() {
    return proxy.getContainerIpAddress();
  }

  public int getZookeeperPort() {
    return ZOOKEEPER_PORT;
//    return proxy.getMappedPort(ZOOKEEPER.getOriginalPort());
  }

  @Override
  public void stop() {
    Stream.<Runnable>of(super::stop, proxy::stop).parallel().forEach(Runnable::run);
  }
}
