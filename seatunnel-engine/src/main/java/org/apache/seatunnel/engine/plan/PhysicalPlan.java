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

import com.google.common.collect.ImmutableMap;
import java.util.List;
import org.apache.seatunnel.engine.dag.LogicalDag;
import org.apache.seatunnel.engine.dag.LogicalEdge;
import org.apache.seatunnel.engine.dag.PlanValidationException;

/** Immutable lowered topology. Exchanges stay compact rather than expanding quadratic channels. */
public final class PhysicalPlan {
  private final LogicalDag logicalDag;
  private final List<TaskGroup> taskGroups;
  private final ImmutableMap<String, TaskGroup> groupsById;
  private final long totalTasks;

  PhysicalPlan(LogicalDag logicalDag, List<TaskGroup> taskGroups) {
    this.logicalDag = logicalDag;
    this.taskGroups = List.copyOf(taskGroups);
    groupsById = taskGroups.stream().collect(ImmutableMap.toImmutableMap(TaskGroup::id, group -> group));
    totalTasks = taskGroups.stream().mapToLong(group -> group.tasks().size()).sum();
  }

  public LogicalDag logicalDag() {
    return logicalDag;
  }

  public List<TaskGroup> taskGroups() {
    return taskGroups;
  }

  public List<LogicalEdge> edges() {
    return logicalDag.edges();
  }

  public long totalTasks() {
    return totalTasks;
  }

  public TaskGroup group(String operatorId) {
    TaskGroup group = groupsById.get(java.util.Objects.requireNonNull(operatorId, "operatorId"));
    if (group == null) {
      throw new PlanValidationException("Unknown task group: " + operatorId);
    }
    return group;
  }

  /** Potential downstream task connectivity; per-record routing is a future runtime concern. */
  public List<TaskId> targetTasks(TaskId producer, String consumerId) {
    TaskGroup source = group(producer.operatorId());
    TaskGroup target = group(consumerId);
    if (producer.subtaskIndex() >= source.tasks().size()) {
      throw new PlanValidationException("Producer subtask is out of range: " + producer);
    }
    return switch (logicalDag.exchange(source.id(), target.id())) {
      case FORWARD -> List.of(target.tasks().get(producer.subtaskIndex()));
      case SHUFFLE -> target.tasks();
    };
  }
}
