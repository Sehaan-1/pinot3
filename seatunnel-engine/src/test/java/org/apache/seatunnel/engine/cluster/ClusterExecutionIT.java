/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.engine.cluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.engine.dag.LogicalDag;
import org.apache.seatunnel.engine.dag.LogicalEdge;
import org.apache.seatunnel.engine.dag.LogicalNode;
import org.apache.seatunnel.engine.plan.PhysicalPlan;
import org.apache.seatunnel.engine.plan.PhysicalPlanner;
import org.apache.seatunnel.engine.plan.TaskId;
import org.apache.seatunnel.engine.task.EngineTask;
import org.apache.seatunnel.engine.task.TaskContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real TCP members and native Hazelcast remote execution; no mocked core paths. */
class ClusterExecutionIT {
  private static final Duration WAIT = Duration.ofSeconds(20);
  private static final String CLUSTER = "seatunnel-it-" + UUID.randomUUID();
  private static EngineNode leader;
  private static EngineNode worker;
  private static SimpleMeterRegistry leaderMetrics;
  private static SimpleMeterRegistry workerMetrics;

  private static EngineSettings settings(List<String> seeds) {
    return new EngineSettings(CLUSTER, "127.0.0.1", 0, seeds, 2, 16, 1, Duration.ofSeconds(5));
  }

  @BeforeAll
  static void startMembers() throws Exception {
    leaderMetrics = new SimpleMeterRegistry();
    workerMetrics = new SimpleMeterRegistry();
    try {
      leader = EngineNode.start(settings(List.of()), leaderMetrics);
      worker = EngineNode.start(settings(List.of(seed())), workerMetrics);
      await().atMost(WAIT).untilAsserted(() -> {
        assertThat(leader.member().getCluster().getMembers()).hasSize(2);
        assertThat(worker.member().getCluster().getMembers()).hasSize(2);
      });
    } catch (Throwable failure) {
      stopMembers();
      throw failure;
    }
  }

  private static String seed() {
    return "127.0.0.1:" + leader.member().getCluster().getLocalMember().getAddress().getPort();
  }

  @AfterAll
  static void stopMembers() throws Exception {
    try {
      if (worker != null) { worker.close(); }
    } finally {
      try {
        if (leader != null) { leader.close(); }
      } finally {
        if (workerMetrics != null) { workerMetrics.close(); }
        if (leaderMetrics != null) { leaderMetrics.close(); }
      }
    }
  }

  private PhysicalPlan plan() {
    CatalogTable table = new CatalogTable("events", List.of(new Column("id", DataType.LONG, false)));
    return new PhysicalPlanner().plan(new LogicalDag(List.of(
        new LogicalNode("s", LogicalNode.Kind.SOURCE, "test-source", 1, table),
        new LogicalNode("t", LogicalNode.Kind.SINK, "test-sink", 1, table)),
        List.of(new LogicalEdge("s", "t", LogicalEdge.Exchange.FORWARD))));
  }

  private Map<TaskId, EngineTask> bindings(EngineTask task) {
    return Map.of(new TaskId("s", 0), task, new TaskId("t", 0), task);
  }

  private UUID workerId() {
    return worker.member().getCluster().getLocalMember().getUuid();
  }

  private record RecordTask(String resultMap) implements EngineTask {
    @Override
    public void run(TaskContext context) {
      long sum = 0;
      for (int i = 0; i < 1000; i++) { sum += i; }
      context.cluster().getMap(resultMap).put(context.key().taskId(), sum);
    }
  }

  private record BlockingTask(String signals) implements EngineTask {
    @Override
    public void run(TaskContext context) throws Exception {
      var queue = context.cluster().<String>getQueue(signals);
      queue.put("started");
      try { Thread.sleep(TimeUnit.MINUTES.toMillis(1)); }
      finally { queue.offer("stopped"); }
    }
  }

  private record FailingTask() implements EngineTask {
    @Override
    public void run(TaskContext context) { throw new IllegalStateException("intentional remote failure"); }
  }

  @Test
  void executesBoundPhysicalTasksOnTheOtherMemberAndReportsRealMetrics() throws Exception {
    String map = "results-" + UUID.randomUUID();
    try (var job = leader.master().submit(plan(), bindings(new RecordTask(map)), workerId())) {
      var outcomes = job.await(WAIT);
      assertThat(outcomes).hasSize(2);
      assertThat(outcomes).allSatisfy(result -> assertThat(result.workerId()).isEqualTo(workerId()));
      assertThat(leader.member().getMap(map)).hasSize(2).containsValue(499500L);
      assertThat(job.status()).isEqualTo(JobExecution.Status.SUCCEEDED);
      await().atMost(WAIT).untilAsserted(() ->
          assertThat(workerMetrics.get("seatunnel.task.succeeded").counter().count()).isGreaterThanOrEqualTo(2));
    } finally { leader.member().getMap(map).destroy(); }
  }

  @Test
  void validatesLeadershipBindingsAndPinnedWorkerBeforeDispatch() {
    assertThat(leader.master().isLeader()).isTrue();
    assertThat(worker.master().isLeader()).isFalse();
    var tasks = bindings(new RecordTask("unused"));
    assertThatThrownBy(() -> worker.master().submit(plan(), tasks))
        .isInstanceOf(IllegalStateException.class).hasMessageContaining("oldest");
    assertThatThrownBy(() -> leader.master().submit(plan(), Map.of()))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Bindings");
    assertThatThrownBy(() -> leader.master().submit(plan(), tasks, UUID.randomUUID()))
        .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unknown engine worker");
  }

  @Test
  void propagatesRemoteFailureAndInterruptsRunningSibling() throws Exception {
    String signals = "failure-" + UUID.randomUUID();
    Map<TaskId, EngineTask> tasks = new HashMap<>(bindings(new BlockingTask(signals)));
    tasks.put(new TaskId("s", 0), new FailingTask());
    var queue = leader.member().<String>getQueue(signals);
    try (var job = leader.master().submit(plan(), tasks, workerId())) {
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("started");
      assertThatThrownBy(() -> job.await(WAIT)).isInstanceOf(ExecutionException.class)
          .hasCauseInstanceOf(IllegalStateException.class);
      assertThat(job.status()).isEqualTo(JobExecution.Status.FAILED);
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("stopped");
    } finally { queue.destroy(); }
  }

  @Test
  void cancelsRemoteWorkAndEnforcesMasterAdmissionLimit() throws Exception {
    String signals = "cancel-" + UUID.randomUUID();
    var queue = leader.member().<String>getQueue(signals);
    try (var job = leader.master().submit(plan(), bindings(new BlockingTask(signals)), workerId())) {
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("started");
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("started");
      assertThatThrownBy(() -> leader.master().submit(plan(), bindings(new RecordTask("unused"))))
          .isInstanceOf(RejectedExecutionException.class).hasMessageContaining("admission");
      assertThat(job.cancel()).isTrue();
      assertThat(job.status()).isEqualTo(JobExecution.Status.CANCELLED);
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("stopped");
      assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("stopped");
    } finally { queue.destroy(); }
  }

  @Test
  void workerDepartureFailsItsJobWithoutShuttingDownOtherMembersOrCallerRegistry() throws Exception {
    SimpleMeterRegistry metrics = new SimpleMeterRegistry();
    EngineNode departing = EngineNode.start(settings(List.of(seed())), metrics);
    String signals = "departure-" + UUID.randomUUID();
    var queue = leader.member().<String>getQueue(signals);
    try {
      await().atMost(WAIT).untilAsserted(() -> assertThat(leader.member().getCluster().getMembers()).hasSize(3));
      UUID target = departing.member().getCluster().getLocalMember().getUuid();
      try (var job = leader.master().submit(plan(), bindings(new BlockingTask(signals)), target)) {
        assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("started");
        assertThat(queue.poll(20, TimeUnit.SECONDS)).isEqualTo("started");
        departing.close();
        assertThatThrownBy(() -> job.await(WAIT)).isInstanceOf(ExecutionException.class);
        assertThat(leader.member().getLifecycleService().isRunning()).isTrue();
        assertThat(worker.member().getLifecycleService().isRunning()).isTrue();
        assertThat(metrics.isClosed()).isFalse();
        assertThat(metrics.getMeters()).isEmpty();
        assertThatThrownBy(departing::member).isInstanceOf(IllegalStateException.class);
      }
    } finally {
      departing.close();
      metrics.close();
      queue.destroy();
    }
  }
}
