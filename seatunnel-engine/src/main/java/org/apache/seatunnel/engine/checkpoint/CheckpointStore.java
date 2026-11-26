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

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.EntryProcessor;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** In-memory replicated decisions; single healthy-cluster coordinator per job is required. */
public final class CheckpointStore {
  public static final String MAP_NAME = "seatunnel.checkpoints.v1";
  private final IMap<UUID, CheckpointRecord> records;

  /** Call before starting a member. Default synchronous backup, no TTL or eviction. */
  public static MapConfig mapConfig() {
    return new MapConfig(MAP_NAME)
        .setBackupCount(1)
        .setAsyncBackupCount(0)
        .setTimeToLiveSeconds(0)
        .setMaxIdleSeconds(0);
  }

  public CheckpointStore(HazelcastInstance member) {
    records = Objects.requireNonNull(member, "member").getMap(MAP_NAME);
  }

  public CheckpointRecord get(UUID jobId) {
    return records.get(Objects.requireNonNull(jobId, "jobId"));
  }

  boolean publish(UUID jobId, CheckpointRecord expected, CheckpointRecord decision) {
    return records.executeOnKey(jobId, new Transition(expected, decision));
  }

  void complete(UUID jobId, CheckpointRecord decision) {
    if (!records.executeOnKey(jobId, new Transition(decision, decision.completed()))) {
      CheckpointRecord current = get(jobId);
      if (current == null || current.id() != decision.id()
          || current.status() != CheckpointRecord.Status.COMPLETED)
        throw new IllegalStateException("Checkpoint decision changed during completion");
    }
  }

  /** Explicit lifecycle cleanup. Never remove an unresolved external commit decision. */
  public void delete(UUID jobId) {
    CheckpointRecord record = get(jobId);
    if (record == null) return;
    if (record.status() != CheckpointRecord.Status.COMPLETED)
      throw new IllegalStateException("Recover pending checkpoint before deleting job");
    if (!records.executeOnKey(jobId, new Transition(record, null)))
      throw new IllegalStateException("Checkpoint changed");
  }

  /** Compare immutable decision versions, not arbitrary user payload equals/serialization bytes. */
  private static final class Transition implements EntryProcessor<UUID, CheckpointRecord, Boolean> {
    private final long expectedId;
    private final CheckpointRecord.Status expectedStatus;
    private final CheckpointRecord replacement;

    Transition(CheckpointRecord expected, CheckpointRecord replacement) {
      expectedId = expected == null ? 0 : expected.id();
      expectedStatus = expected == null ? null : expected.status();
      this.replacement = replacement;
    }

    @Override
    public Boolean process(Map.Entry<UUID, CheckpointRecord> entry) {
      CheckpointRecord current = entry.getValue();
      boolean matches = current == null ? expectedId == 0
          : current.id() == expectedId && current.status() == expectedStatus;
      if (matches) entry.setValue(replacement);
      return matches;
    }
  }
}
