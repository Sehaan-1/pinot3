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
package org.apache.seatunnel.engine.dag;

import java.util.Objects;
import org.apache.seatunnel.api.table.CatalogTable;

/** Immutable planning metadata; no live operators or execution-engine handles. */
public record LogicalNode(String id, Kind kind, String pluginId, int parallelism,
    CatalogTable inputTable, CatalogTable outputTable) {
  public LogicalNode {
    if (Objects.requireNonNull(id, "id").isBlank()) {
      throw new PlanValidationException("Operator ID must not be blank");
    }
    Objects.requireNonNull(kind, "kind");
    if (!Objects.requireNonNull(pluginId, "pluginId").matches("[a-z][a-z0-9-]*")) {
      throw new PlanValidationException("Invalid plugin identifier: " + pluginId);
    }
    if (parallelism <= 0) {
      throw new PlanValidationException("Parallelism must be positive for " + id);
    }
    switch (kind) {
      case SOURCE -> {
        if (inputTable != null || outputTable == null) {
          throw new PlanValidationException("Source requires only an output schema: " + id);
        }
      }
      case TRANSFORM -> {
        if (inputTable == null || outputTable == null) {
          throw new PlanValidationException("Transform requires input and output schemas: " + id);
        }
      }
      case SINK -> {
        if (inputTable == null || outputTable != null) {
          throw new PlanValidationException("Sink requires only an input schema: " + id);
        }
      }
    }
  }

  /** Convenience constructor for sources, sinks and schema-preserving transforms. */
  public LogicalNode(String id, Kind kind, String pluginId, int parallelism, CatalogTable table) {
    this(id, kind, pluginId, parallelism,
        kind == Kind.SOURCE ? null : table, kind == Kind.SINK ? null : table);
  }

  public enum Kind {
    SOURCE,
    TRANSFORM,
    SINK
  }
}
