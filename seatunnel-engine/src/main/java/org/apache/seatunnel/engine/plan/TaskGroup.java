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
import org.apache.seatunnel.engine.dag.LogicalNode;
import org.apache.seatunnel.engine.dag.PlanValidationException;

/** One operator's contiguous subtasks, not a fused pipeline or a worker assignment. */
public record TaskGroup(LogicalNode operator, List<TaskId> tasks) {
  public TaskGroup {
    Objects.requireNonNull(operator, "operator");
    tasks = List.copyOf(tasks);
    if (tasks.size() != operator.parallelism()) {
      throw new PlanValidationException("Task count does not match parallelism: " + operator.id());
    }
    for (int i = 0; i < tasks.size(); i++) {
      TaskId task = tasks.get(i);
      if (!task.operatorId().equals(operator.id()) || task.subtaskIndex() != i) {
        throw new PlanValidationException("Task ownership or index order is invalid: " + operator.id());
      }
    }
  }

  public String id() {
    return operator.id();
  }
}
