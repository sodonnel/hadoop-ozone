/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import org.apache.hadoop.hdds.client.ReplicationFactor;
import org.apache.hadoop.hdds.client.ReplicationType;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.common.helpers.ContainerWithPipeline;
import org.apache.hadoop.hdds.scm.PlacementPolicy;
import org.apache.hadoop.hdds.scm.container.placement.algorithms.SCMContainerPlacementCapacity;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.protocolPB.StorageContainerLocationProtocolClientSideTranslatorPB;
import org.apache.hadoop.ozone.client.OzoneBucket;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.junit.Rule;
import org.junit.rules.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * This class tests the StorageContainerLocationProtocol from the client side
 */
public class TestStorageContainerLocationClient {

  /**
   * Set a timeout for each test.
   */
  @Rule
  public Timeout timeout = new Timeout(300000);

  private static StorageContainerLocationProtocolClientSideTranslatorPB client;
  private static MiniOzoneCluster cluster;
  private static OzoneConfiguration ozoneConf;

  @Before
  public void setup() throws Exception {
    ozoneConf = new OzoneConfiguration();
    ozoneConf.setClass(ScmConfigKeys.OZONE_SCM_CONTAINER_PLACEMENT_IMPL_KEY,
        SCMContainerPlacementCapacity.class, PlacementPolicy.class);
    cluster = MiniOzoneCluster.newBuilder(ozoneConf).setNumDatanodes(3).build();
    client = cluster.getStorageContainerLocationClient();
    cluster.waitForClusterToBeReady();
    OzoneBucket bucket = TestDataUtil.createVolumeAndBucket(cluster, "vol1",
        "bucket1");
    TestDataUtil.createKey(bucket, "key1", ReplicationFactor.THREE,
        ReplicationType.RATIS, "Key contents");
  }

  @After
  public void cleanup() {
    if(cluster != null) {
      cluster.shutdown();
    }
  }

  /**
   * Retrieve the ContainerWithPipeline with the sorted flag false and true.
   * @throws Exception
   */
  @Test
  public void testGetContainerWithPipeline() throws Exception {
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainers().get(0);
    ContainerWithPipeline cwp = client.getContainerWithPipeline(
        container.getContainerID(), false, null);

    assertEquals(container.containerID().getId(),
        cwp.getContainerInfo().getContainerID());
    assertEquals(false, cwp.getPipeline().nodesAreOrdered());

    String clientHost = cluster.getHddsDatanodes().get(0).getDatanodeDetails()
        .getUuid().toString();
    cwp = client.getContainerWithPipeline(
        container.getContainerID(), true, clientHost);
    assertEquals(container.containerID().getId(),
        cwp.getContainerInfo().getContainerID());
    assertEquals(true, cwp.getPipeline().nodesAreOrdered());
  }

  /**
   * Retrieve the ContainerWithPipeline with the sorted flag false and true.
   * @throws Exception
   */
  @Test
  public void testGetContainerWithPipelineBatch() throws Exception {
    ContainerInfo container = cluster.getStorageContainerManager()
        .getContainerManager().getContainers().get(0);
    List<Long> ids = new ArrayList<>();
    ids.add(container.getContainerID());
    List<ContainerWithPipeline> cwps = client.getContainerWithPipelineBatch(
        ids, false, null);

    ContainerWithPipeline cwp = cwps.get(0);
    assertEquals(container.containerID().getId(),
        cwp.getContainerInfo().getContainerID());
    assertEquals(false, cwp.getPipeline().nodesAreOrdered());

    String clientHost = cluster.getHddsDatanodes().get(0).getDatanodeDetails()
        .getUuid().toString();
    cwps = client.getContainerWithPipelineBatch(
        ids, true, clientHost);
    cwp = cwps.get(0);
    assertEquals(container.containerID().getId(),
        cwp.getContainerInfo().getContainerID());
    assertEquals(true, cwp.getPipeline().nodesAreOrdered());
  }

}

