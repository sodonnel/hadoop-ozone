/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership.  The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.hadoop.hdds.scm.node;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.metrics2.lib.Interns;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;
import org.apache.hadoop.ozone.OzoneConsts;

import java.util.HashMap;
import java.util.Map;

/**
 * Class contains metrics related to the NodeDecommissionManager.
 */
@Metrics(about = "Node Decommission Metrics", context = OzoneConsts.OZONE)
public final class NodeDecommissionMetrics implements MetricsSource {
  public static final String METRICS_SOURCE_NAME =
      org.apache.hadoop.hdds.scm.node.NodeDecommissionMetrics
          .class.getSimpleName();

  @Metric("Number of nodes tracked for decommissioning and maintenance.")
  private MutableGaugeLong trackedDecommissioningMaintenanceNodesTotal;

  @Metric("Number of nodes tracked for recommissioning.")
  private MutableGaugeLong trackedRecommissionNodesTotal;

  @Metric("Number of nodes tracked with pipelines waiting to close.")
  private MutableGaugeLong trackedPipelinesWaitingToCloseTotal;

  @Metric("Number of containers under replicated in tracked nodes.")
  private MutableGaugeLong trackedContainersUnderReplicatedTotal;

  @Metric("Number of containers unhealthy in tracked nodes.")
  private MutableGaugeLong trackedContainersUnhealthyTotal;

  @Metric("Number of containers sufficiently replicated in tracked nodes.")
  private MutableGaugeLong trackedContainersSufficientlyReplicatedTotal;

  private MetricsRegistry registry;

  public enum MetricByHost {
    PipelinesWaitingToClose("TrackedPipelinesWaitingToClose",
        "Number of pipelines waiting to close for "
            + "host in decommissioning and "
            + "maintenance mode"),
    SufficientlyReplicated("TrackedSufficientlyReplicated",
        "Number of sufficiently replicated containers "
            + "for host in decommissioning and "
            + "maintenance mode"),
    UnderReplicated("TrackedUnderReplicated",
        "Number of under-replicated containers "
            + "for host in decommissioning and "
            + "maintenance mode"),
    UnhealthyContainers("TrackedUnhealthyContainers",
        "Number of unhealthy containers "
            + "for host in decommissioning and "
            + "maintenance mode");

    private final String metricName;
    private final String desc;

    MetricByHost(String name, String desc) {
      this.metricName = name;
      this.desc = desc;
    }

    public String getMetricName(String host) {
      return metricName + "-" + host;
    }

    public String getDescription() {
      return desc;
    }
  };

  private Map<MetricsInfo, Long> hostMetrics = new HashMap<>();

  /** Private constructor. */
  private NodeDecommissionMetrics() {
    this.registry = new MetricsRegistry(METRICS_SOURCE_NAME);
  }

  /**
   * Create and returns NodeDecommissionMetrics instance.
   *
   * @return NodeDecommissionMetrics
   */
  public static NodeDecommissionMetrics create() {
    return DefaultMetricsSystem.instance().register(METRICS_SOURCE_NAME,
        "Metrics tracking the progress of nodes in the "
            + "Decommissioning and Maintenance workflows.  "
            + "Tracks num nodes in mode and container "
            + "replications state and pipelines waiting to close",
        new NodeDecommissionMetrics());
  }

  /**
   * Get aggregated gauge metrics.
   */
  @Override
  public synchronized void getMetrics(MetricsCollector collector, boolean all) {
    MetricsRecordBuilder builder = collector
        .addRecord(METRICS_SOURCE_NAME);
    trackedDecommissioningMaintenanceNodesTotal.snapshot(builder, all);
    trackedRecommissionNodesTotal.snapshot(builder, all);
    trackedPipelinesWaitingToCloseTotal.snapshot(builder, all);
    trackedContainersUnderReplicatedTotal.snapshot(builder, all);
    trackedContainersUnhealthyTotal.snapshot(builder, all);
    trackedContainersSufficientlyReplicatedTotal.snapshot(builder, all);

    for (Map.Entry<MetricsInfo, Long> e : hostMetrics.entrySet()) {
      builder.addGauge(e.getKey(), e.getValue());
    }
  }

  /**
   * Unregister the metrics instance.
   */
  public void unRegister() {
    DefaultMetricsSystem.instance().unregisterSource(METRICS_SOURCE_NAME);
  }

  public synchronized void setTrackedDecommissioningMaintenanceNodesTotal(
            long numNodesTracked) {
    trackedDecommissioningMaintenanceNodesTotal
        .set(numNodesTracked);
  }

  public synchronized void setTrackedRecommissionNodesTotal(
          long numNodesTrackedRecommissioned) {
    trackedRecommissionNodesTotal.set(numNodesTrackedRecommissioned);
  }

  public synchronized void setTrackedPipelinesWaitingToCloseTotal(
          long numTrackedPipelinesWaitToClose) {
    trackedPipelinesWaitingToCloseTotal
        .set(numTrackedPipelinesWaitToClose);
  }

  public synchronized void setTrackedContainersUnderReplicatedTotal(
          long numTrackedUnderReplicated) {
    trackedContainersUnderReplicatedTotal
        .set(numTrackedUnderReplicated);
  }

  public synchronized void setTrackedContainersUnhealthyTotal(
          long numTrackedUnhealthy) {
    trackedContainersUnhealthyTotal
        .set(numTrackedUnhealthy);
  }

  public synchronized void setTrackedContainersSufficientlyReplicatedTotal(
          long numTrackedSufficientlyReplicated) {
    trackedContainersSufficientlyReplicatedTotal
        .set(numTrackedSufficientlyReplicated);
  }

  public synchronized long getTrackedDecommissioningMaintenanceNodesTotal() {
    return trackedDecommissioningMaintenanceNodesTotal.value();
  }

  public synchronized long getTrackedRecommissionNodesTotal() {
    return trackedRecommissionNodesTotal.value();
  }

  public synchronized long getTrackedPipelinesWaitingToCloseTotal() {
    return trackedPipelinesWaitingToCloseTotal.value();
  }

  public synchronized long getTrackedContainersUnderReplicatedTotal() {
    return trackedContainersUnderReplicatedTotal.value();
  }

  public synchronized long getTrackedContainersUnhealthyTotal() {
    return trackedContainersUnhealthyTotal.value();
  }

  public synchronized long getTrackedContainersSufficientlyReplicatedTotal() {
    return trackedContainersSufficientlyReplicatedTotal.value();
  }

  public synchronized void setHostLevelMetrics(Map<String,
      DatanodeAdminMonitorImpl.ContainerStateInWorkflow> metrics) {
    for (DatanodeAdminMonitorImpl.ContainerStateInWorkflow metric :
        metrics.values())  {
      hostMetrics.clear();
      hostMetrics.put(Interns.info(
          MetricByHost.SufficientlyReplicated.getMetricName(metric.getHost()),
          MetricByHost.SufficientlyReplicated.getDescription()),
          metric.getSufficientlyReplicated());
      hostMetrics.put(Interns.info(
              MetricByHost.UnderReplicated.getMetricName(metric.getHost()),
              MetricByHost.UnderReplicated.getDescription()),
          metric.getUnderReplicatedContainers());
      hostMetrics.put(Interns.info(
              MetricByHost.UnhealthyContainers.getMetricName(metric.getHost()),
              MetricByHost.UnhealthyContainers.getDescription()),
          metric.getUnhealthyContainers());
      hostMetrics.put(Interns.info(
              MetricByHost.PipelinesWaitingToClose.getMetricName(
                  metric.getHost()),
              MetricByHost.PipelinesWaitingToClose.getDescription()),
          metric.getPipelinesWaitingToClose());
    }
  }

  public synchronized Long getHostMetric(MetricsInfo metricInfo) {
    return hostMetrics.get(metricInfo);
  }

}
