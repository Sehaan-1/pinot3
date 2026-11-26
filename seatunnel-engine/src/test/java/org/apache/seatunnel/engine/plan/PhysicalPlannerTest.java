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
package org.apache.seatunnel.engine.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.engine.dag.LogicalDag;
import org.apache.seatunnel.engine.dag.LogicalEdge;
import org.apache.seatunnel.engine.dag.LogicalEdge.Exchange;
import org.apache.seatunnel.engine.dag.LogicalNode;
import org.apache.seatunnel.engine.dag.LogicalNode.Kind;
import org.apache.seatunnel.engine.dag.PlanValidationException;
import org.junit.jupiter.api.Test;

class PhysicalPlannerTest {
  private static final CatalogTable TABLE =
      new CatalogTable("events", List.of(new Column("id", DataType.LONG, false)));

  private LogicalNode node(String id, Kind kind, int parallelism) {
    return new LogicalNode(id, kind, "test-plugin", parallelism, TABLE);
  }

  private LogicalDag pair(int sourceParallelism, int sinkParallelism, Exchange exchange) {
    return new LogicalDag(
        List.of(node("s", Kind.SOURCE, sourceParallelism), node("t", Kind.SINK, sinkParallelism)),
        List.of(new LogicalEdge("s", "t", exchange)));
  }

  @Test
  void generatesCompleteStableTaskGroupsAndPointwiseConnectivity() {
    LogicalDag dag = pair(3, 3, Exchange.FORWARD);
    PhysicalPlan plan = new PhysicalPlanner().plan(dag);
    assertThat(plan.logicalDag()).isSameAs(dag);
    assertThat(plan.taskGroups()).extracting(TaskGroup::id).containsExactly("s", "t");
    assertThat(plan.totalTasks()).isEqualTo(6);
    assertThat(plan.group("s").tasks())
        .containsExactly(new TaskId("s", 0), new TaskId("s", 1), new TaskId("s", 2));
    assertThat(plan.targetTasks(new TaskId("s", 1), "t")).containsExactly(new TaskId("t", 1));
    assertThat(new PhysicalPlanner().plan(dag).taskGroups()).isEqualTo(plan.taskGroups());
  }

  @Test
  void shuffleDescribesAllToAllConnectivityWithoutExpandingChannels() {
    PhysicalPlan plan = new PhysicalPlanner().plan(pair(2, 4, Exchange.SHUFFLE));
    assertThat(plan.edges()).hasSize(1);
    for (int i = 0; i < 2; i++) {
      assertThat(plan.targetTasks(new TaskId("s", i), "t")).isEqualTo(plan.group("t").tasks());
    }
    assertThat(plan.totalTasks()).isEqualTo(6);
  }

  @Test
  void preservesFanOutEdgesAcrossDifferentExchangeTypes() {
    LogicalDag dag =
        new LogicalDag(
            List.of(node("s", Kind.SOURCE, 2), node("a", Kind.SINK, 2), node("b", Kind.SINK, 3)),
            List.of(
                new LogicalEdge("s", "a", Exchange.FORWARD),
                new LogicalEdge("s", "b", Exchange.SHUFFLE)));
    PhysicalPlan plan = new PhysicalPlanner().plan(dag);
    assertThat(plan.taskGroups()).hasSize(3);
    assertThat(plan.totalTasks()).isEqualTo(7);
    assertThat(plan.targetTasks(new TaskId("s", 0), "a")).containsExactly(new TaskId("a", 0));
    assertThat(plan.targetTasks(new TaskId("s", 0), "b")).hasSize(3);
  }

  @Test
  void rejectsUnknownGroupsNonexistentEdgesAndInvalidProducerIndices() {
    PhysicalPlan plan = new PhysicalPlanner().plan(pair(2, 2, Exchange.FORWARD));
    assertThatThrownBy(() -> plan.group("unknown")).isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> plan.targetTasks(new TaskId("s", 2), "t"))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("out of range");
    assertThatThrownBy(() -> plan.targetTasks(new TaskId("t", 0), "s"))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("No edge");
    assertThatThrownBy(() -> plan.targetTasks(new TaskId("unknown", 0), "t"))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new TaskId("s", -1)).isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new TaskId(" ", 0)).isInstanceOf(PlanValidationException.class);
  }

  @Test
  void taskGroupsValidateOwnershipCountAndContiguousIndicesAndCopyInputs() {
    LogicalNode operator = node("s", Kind.SOURCE, 2);
    List<TaskId> tasks = new ArrayList<>(List.of(new TaskId("s", 0), new TaskId("s", 1)));
    TaskGroup group = new TaskGroup(operator, tasks);
    tasks.clear();
    assertThat(group.tasks()).hasSize(2);
    assertThatThrownBy(() -> new TaskGroup(operator, List.of(new TaskId("s", 0))))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(
            () -> new TaskGroup(operator, List.of(new TaskId("s", 0), new TaskId("s", 0))))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(
            () -> new TaskGroup(operator, List.of(new TaskId("x", 0), new TaskId("x", 1))))
        .isInstanceOf(PlanValidationException.class);
  }

  @Test
  void exposedPlanCollectionsCannotBeMutated() {
    PhysicalPlan plan = new PhysicalPlanner().plan(pair(2, 2, Exchange.SHUFFLE));
    assertThatThrownBy(() -> plan.taskGroups().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> plan.group("s").tasks().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> plan.edges().clear())
        .isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> plan.targetTasks(new TaskId("s", 0), "t").clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void enforcesTaskBudgetBeforeAllocationWithoutIntegerOverflow() {
    assertThatThrownBy(() -> new PhysicalPlanner(0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new PhysicalPlanner(3).plan(pair(2, 2, Exchange.FORWARD)))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Task budget exceeded");
    assertThat(new PhysicalPlanner(4).plan(pair(2, 2, Exchange.FORWARD)).totalTasks()).isEqualTo(4);
    LogicalDag huge = pair(Integer.MAX_VALUE, Integer.MAX_VALUE, Exchange.FORWARD);
    assertThatThrownBy(() -> new PhysicalPlanner().plan(huge))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("4294967294");
  }

  @Test
  void plannerAndLogicalSnapshotsCanBeReusedConcurrently() throws Exception {
    LogicalDag dag = pair(2, 3, Exchange.SHUFFLE);
    PhysicalPlanner planner = new PhysicalPlanner();
    List<TaskGroup> expected = planner.plan(dag).taskGroups();
    var executor = Executors.newFixedThreadPool(4);
    try {
      List<Callable<List<TaskGroup>>> calls = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        calls.add(() -> planner.plan(dag).taskGroups());
      }
      for (var result : executor.invokeAll(calls)) {
        assertThat(result.get()).isEqualTo(expected);
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
