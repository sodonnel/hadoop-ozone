package org.apache.hadoop.hdds.scm.pipeline;

import org.apache.hadoop.hdds.client.ECReplicationConfig;
import org.apache.hadoop.hdds.client.ReplicationConfig;
import org.apache.hadoop.hdds.conf.ConfigurationSource;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.proto.HddsProtos;
import org.apache.hadoop.hdds.scm.PipelineChoosePolicy;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.container.ContainerID;
import org.apache.hadoop.hdds.scm.container.ContainerInfo;
import org.apache.hadoop.hdds.scm.container.ContainerManagerV2;
import org.apache.hadoop.hdds.scm.container.ContainerNotFoundException;
import org.apache.hadoop.hdds.scm.container.common.helpers.ExcludeList;
import org.apache.hadoop.hdds.scm.pipeline.choose.algorithms.HealthyPipelineChoosePolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;

import static org.apache.hadoop.hdds.conf.StorageUnit.BYTES;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.CLOSED;
import static org.apache.hadoop.hdds.scm.pipeline.Pipeline.PipelineState.OPEN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests to validate the WritableECContainerProvider works correctly.
 */
public class TestWritableECContainerProvider {

  private static final Logger LOG = LoggerFactory
      .getLogger(TestWritableECContainerProvider.class);
  private static final String OWNER = "SCM";
  private PipelineManager pipelineManager = MockPipelineManager.getInstance();
  private ContainerManagerV2 containerManager
      = Mockito.mock(ContainerManagerV2.class);
  private PipelineChoosePolicy pipelineChoosingPolicy
      = new HealthyPipelineChoosePolicy();

  private ConfigurationSource conf;
  private WritableContainerProvider provider;
  private ReplicationConfig repConfig;

  private Map<ContainerID, ContainerInfo> containers;

  @Before
  public void setup() throws ContainerNotFoundException {
    repConfig = new ECReplicationConfig(3, 2);
    conf = new OzoneConfiguration();
    containers = new HashMap<>();
    provider = new WritableECContainerProvider(
        conf, pipelineManager, containerManager, pipelineChoosingPolicy);

    Mockito.doAnswer(call -> {
      Pipeline pipeline = (Pipeline)call.getArguments()[2];
      ContainerInfo container = createContainer(pipeline,
          repConfig, System.nanoTime());
      pipelineManager.addContainerToPipeline(
          pipeline.getId(), container.containerID());
      containers.put(container.containerID(), container);
      return container;
    }).when(containerManager).getMatchingContainer(Matchers.anyLong(),
        Matchers.anyString(), Matchers.any(Pipeline.class));

    Mockito.doAnswer(call ->
        containers.get((ContainerID)call.getArguments()[0]))
        .when(containerManager).getContainer(Matchers.any(ContainerID.class));

  }

  @After
  public void teardown() {
  }

  @Test
  public void testPipelinesCreatedUpToMinLimitAndRandomPipelineReturned()
      throws IOException {
    // The first 5 calls should return a different container
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
    }

    allocatedContainers.clear();
    for (int i=0; i<20; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // Should have 5 containers created
    assertEquals(5, pipelineManager.getPipelines(repConfig, OPEN).size());
    // We should have more than 1 allocatedContainers in the set proving a
    // random container is selected each time. Do not check for 5 here as there
    // is a reasonable chance that in 20 turns we don't pick all 5 nodes.
    assertTrue(allocatedContainers.size() > 2);
  }

  @Test
  public void testPiplineLimitIgnoresExcludedPipelines() throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude one. It should use
    // one of the existing rather than createing a new one, as the limit is
    // checked against all pipelines, not just the filtered list
    ExcludeList exclude = new ExcludeList();
    PipelineID excludedID = allocatedContainers
        .stream().findFirst().get().getPipelineID();
    exclude.addPipeline(excludedID);

    ContainerInfo c = provider.getContainer(1, repConfig, OWNER, exclude);
    assertNotEquals(excludedID, c.getPipelineID());
    assertTrue(allocatedContainers.contains(c));
  }

  @Test
  public void testNewPipelineCreatedIfAllPipelinesExcluded()
      throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude one. It should use
    // one of the existing rather than createing a new one, as the limit is
    // checked against all pipelines, not just the filtered list
    ExcludeList exclude = new ExcludeList();
    for (ContainerInfo c : allocatedContainers) {
      exclude.addPipeline(c.getPipelineID());
    }
    ContainerInfo c = provider.getContainer(1, repConfig, OWNER, exclude);
    assertFalse(allocatedContainers.contains(c));
  }

  @Test
  public void testNewPipelineCreatedIfAllContainersExcluded()
      throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      allocatedContainers.add(container);
    }
    // We have the min limit of pipelines, but then exclude one. It should use
    // one of the existing rather than createing a new one, as the limit is
    // checked against all pipelines, not just the filtered list
    ExcludeList exclude = new ExcludeList();
    for (ContainerInfo c : allocatedContainers) {
      exclude.addConatinerId(c.containerID());
    }
    ContainerInfo c = provider.getContainer(1, repConfig, OWNER, exclude);
    assertFalse(allocatedContainers.contains(c));
  }

  @Test
  public void testUnableToCreateAnyPipelinesReturnsNull() throws IOException {
    pipelineManager = new MockPipelineManager() {
      @Override
      public Pipeline createPipeline(ReplicationConfig repConf)
          throws IOException {
        throw new IOException("Cannot create pipelines");
      }
    };
    provider = new WritableECContainerProvider(
        conf, pipelineManager, containerManager, pipelineChoosingPolicy);

    ContainerInfo container =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNull(container);
  }

  @Test
  public void testExistingPipelineReturnedWhenNewCannotBeCreated()
      throws IOException {
    pipelineManager = new MockPipelineManager() {

      private boolean throwError = false;

      @Override
      public Pipeline createPipeline(ReplicationConfig repConf)
          throws IOException {
        if (throwError) {
          throw new IOException("Cannot create pipelines");
        }
        throwError = true;
        return super.createPipeline(repConfig);
      }
    };
    provider = new WritableECContainerProvider(
        conf, pipelineManager, containerManager, pipelineChoosingPolicy);

    ContainerInfo container =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    for (int i=0; i<5; i++) {
      ContainerInfo nextContainer =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertEquals(container, nextContainer);
    }
  }

  @Test
  public void testNewContainerAllocatedAndPipelinesClosedIfNoSpaceInExisting()
      throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
    }
    // Update all the containers to make them full
    for (ContainerInfo c : allocatedContainers) {
      c.setUsedBytes(getMaxContainerSize());
    }
    // Get a new container and ensure it is not one of the original set
    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertFalse(allocatedContainers.contains(newContainer));
    // The original pipelines should all be closed
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @Test
  public void testPipelineNotFoundWhenAttemptingToUseExisting()
      throws IOException {
    // Ensure PM throws PNF exception when we ask for the containers in the
    // pipeline
    pipelineManager = new MockPipelineManager() {

      @Override
      public NavigableSet<ContainerID> getContainersInPipeline(
          PipelineID pipelineID) throws IOException {
        throw new PipelineNotFoundException("Simulated exception");
      }
    };
    provider = new WritableECContainerProvider(
        conf, pipelineManager, containerManager, pipelineChoosingPolicy);

    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
    }
    // Now attempt to get a container - any attempt to use an existing with
    // throw PNF and then we must allocate a new one
    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertFalse(allocatedContainers.contains(newContainer));
  }

  @Test
  public void testContainerNotFoundWhenAttemptingToUseExisting()
      throws IOException {
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container =
          provider.getContainer(1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
    }

    // Ensure ContainerManager always throws when a container is requested so
    // existing pipelines cannot be used
    Mockito.doAnswer(call -> {
      throw new ContainerNotFoundException();
    }).when(containerManager).getContainer(Matchers.any(ContainerID.class));

    ContainerInfo newContainer =
        provider.getContainer(1, repConfig, OWNER, new ExcludeList());
    assertNotNull(newContainer);
    assertFalse(allocatedContainers.contains(newContainer));

    // Ensure all the existing pipelines are closed
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  @Test
  public void testPipelineOpenButContainerRemovedFromIt() throws IOException {
    // This can happen if the container close process is triggered from the DN.
    // When tha happens, CM will change the container state to CLOSING and
    // remove it from the container list in pipeline Manager.
    Set<ContainerInfo> allocatedContainers = new HashSet<>();
    for (int i=0; i<5; i++) {
      ContainerInfo container = provider.getContainer(
          1, repConfig, OWNER, new ExcludeList());
      assertFalse(allocatedContainers.contains(container));
      allocatedContainers.add(container);
      // Remove the container from the pipeline to simulate closing it
      pipelineManager.removeContainerFromPipeline(
          container.getPipelineID(), container.containerID());
    }
    ContainerInfo newContainer = provider.getContainer(
        1, repConfig, OWNER, new ExcludeList());
    assertFalse(allocatedContainers.contains(newContainer));
    for (ContainerInfo c : allocatedContainers) {
      Pipeline pipeline = pipelineManager.getPipeline(c.getPipelineID());
      assertEquals(CLOSED, pipeline.getPipelineState());
    }
  }

  private ContainerInfo createContainer(Pipeline pipeline,
      ReplicationConfig repConf, long containerID) {
    return new ContainerInfo.Builder()
        .setContainerID(containerID)
        .setOwner(OWNER)
        .setReplicationConfig(repConf)
        .setState(HddsProtos.LifeCycleState.OPEN)
        .setPipelineID(pipeline.getId())
        .setNumberOfKeys(0)
        .setUsedBytes(0)
        .setSequenceId(0)
        .setDeleteTransactionId(0)
        .build();
  }

  private long getMaxContainerSize() {
    return (long)conf.getStorageSize(
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE,
        ScmConfigKeys.OZONE_SCM_CONTAINER_SIZE_DEFAULT, BYTES);
  }

}
