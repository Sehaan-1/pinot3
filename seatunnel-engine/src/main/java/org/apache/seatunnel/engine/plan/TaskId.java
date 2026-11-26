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

import java.io.Serializable;
import java.util.Objects;
import org.apache.seatunnel.engine.dag.PlanValidationException;

/** Stable operator-local subtask identity; not a network or attempt identifier. */
public record TaskId(String operatorId, int subtaskIndex) implements Serializable {
  public TaskId {
    if (Objects.requireNonNull(operatorId, "operatorId").isBlank() || subtaskIndex < 0) {
      throw new PlanValidationException("Task requires a nonblank operator and nonnegative index");
    }
  }
}
