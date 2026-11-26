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
package org.apache.seatunnel.engine.checkpoint;

import java.io.Serializable;
import java.util.Map;

/** One decision plus the last completed snapshot while committing. No unbounded history. */
public record CheckpointRecord(
    long id, Status status, Map<String, PreparedState> participants, CheckpointRecord previous)
    implements Serializable {
  public enum Status {
    COMMITTING,
    COMPLETED
  }

  public CheckpointRecord {
    if (id <= 0) throw new IllegalArgumentException("checkpoint ID must be positive");
    java.util.Objects.requireNonNull(status, "status");
    participants = Map.copyOf(participants);
    if (participants.isEmpty() || participants.keySet().stream().anyMatch(String::isBlank))
      throw new IllegalArgumentException("nonempty named participants required");
    if (previous != null
        && (previous.status != Status.COMPLETED || previous.id >= id || previous.previous != null))
      throw new IllegalArgumentException("previous must be an older completed snapshot");
    if (status == Status.COMPLETED && previous != null)
      throw new IllegalArgumentException("completed records cannot retain history");
  }

  public CheckpointRecord completed() {
    return new CheckpointRecord(id, Status.COMPLETED, participants, null);
  }
}
