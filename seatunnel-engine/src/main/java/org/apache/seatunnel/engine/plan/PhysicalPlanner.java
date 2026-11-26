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

import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.seatunnel.engine.dag.LogicalDag;
import org.apache.seatunnel.engine.dag.PlanValidationException;

/** Thread-safe deterministic lowering with an explicit allocation budget and no runtime side effects. */
public final class PhysicalPlanner {
  public static final int DEFAULT_MAX_TASKS = 100_000;
  private final int maxTasks;

  public PhysicalPlanner() {
    this(DEFAULT_MAX_TASKS);
  }

  public PhysicalPlanner(int maxTasks) {
    if (maxTasks <= 0) {
      throw new IllegalArgumentException("Task budget must be positive");
    }
    this.maxTasks = maxTasks;
  }

  public PhysicalPlan plan(LogicalDag dag) {
    Objects.requireNonNull(dag, "dag");
    long requested = dag.nodes().stream().mapToLong(node -> node.parallelism()).sum();
    if (requested > maxTasks) {
      throw new PlanValidationException("Task budget exceeded: requested " + requested + ", limit " + maxTasks);
    }
    List<TaskGroup> groups = dag.topologicalOrder().stream().map(node -> new TaskGroup(node,
        IntStream.range(0, node.parallelism()).mapToObj(index -> new TaskId(node.id(), index)).toList())).toList();
    return new PhysicalPlan(dag, groups);
  }
}
