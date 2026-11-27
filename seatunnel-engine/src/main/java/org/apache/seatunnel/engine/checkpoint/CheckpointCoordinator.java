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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Explicit two-phase orchestration; never invoke concurrently with participant operator work. */
public final class CheckpointCoordinator {
  private final UUID jobId;
  private final CheckpointRepository store;
  private final Map<String, CheckpointParticipant> participants;
  private long lastAttempt;
  private boolean publicationUncertain;

  public CheckpointCoordinator(
      UUID jobId, CheckpointRepository store, Map<String, CheckpointParticipant> participants) {
    this.jobId = Objects.requireNonNull(jobId, "jobId");
    this.store = Objects.requireNonNull(store, "store");
    this.participants = Map.copyOf(participants);
    if (participants.isEmpty() || participants.keySet().stream().anyMatch(String::isBlank))
      throw new IllegalArgumentException("nonempty named participants required");
  }

  public synchronized CheckpointRecord checkpoint(long id) throws Exception {
    if (publicationUncertain)
      throw new IllegalStateException(
          "Resolve uncertain publication before replacing this coordinator");
    CheckpointRecord before = store.get(jobId);
    if (id <= lastAttempt || (before != null && id <= before.id()))
      throw new IllegalArgumentException("checkpoint ID must increase");
    if (before != null && before.status() == CheckpointRecord.Status.COMMITTING)
      throw new IllegalStateException("Recover pending checkpoint first");
    lastAttempt = id;
    Map<String, PreparedState> prepared = new LinkedHashMap<>();
    try {
      for (var entry : participants.entrySet())
        prepared.put(
            entry.getKey(), Objects.requireNonNull(entry.getValue().prepare(id), "prepared state"));
    } catch (Exception failure) {
      abort(id, failure);
      throw failure;
    }
    CheckpointRecord decision =
        new CheckpointRecord(id, CheckpointRecord.Status.COMMITTING, prepared, before);
    boolean published;
    try {
      published = store.publish(jobId, before, decision);
    } catch (RuntimeException uncertain) {
      publicationUncertain = true;
      // Do NOT abort: publication may have succeeded even if its acknowledgement was lost.
      // Reading/recovering this job resolves the decision; loss of the cluster cannot be resolved
      // here.
      throw new IllegalStateException(
          "Unknown publication outcome; inspect store and recover, never blindly abort", uncertain);
    }
    if (!published) {
      publicationUncertain = true;
      // A competing coordinator may own the same external transaction ID. Never abort it.
      throw new IllegalStateException(
          "Checkpoint changed; resolve conflicting coordinator before cleanup");
    }
    return finish(decision);
  }

  /** Retry every external commit and notification from the replicated decision. */
  public synchronized CheckpointRecord recover() throws Exception {
    CheckpointRecord record = store.get(jobId);
    if (record == null) return null;
    validate(record);
    return record.status() == CheckpointRecord.Status.COMMITTING ? finish(record) : record;
  }

  /** Restore only fully completed state; call recover first if a decision is pending. */
  public synchronized CheckpointRecord restore() throws Exception {
    CheckpointRecord record = store.get(jobId);
    if (record == null) return null;
    validate(record);
    if (record.status() != CheckpointRecord.Status.COMPLETED)
      throw new IllegalStateException("Recover pending checkpoint before restoring");
    for (var entry : participants.entrySet())
      entry.getValue().restore(record.participants().get(entry.getKey()).state());
    return record;
  }

  private CheckpointRecord finish(CheckpointRecord record) throws Exception {
    validate(record);
    for (var entry : participants.entrySet())
      entry.getValue().commit(record.id(), record.participants().get(entry.getKey()).handles());
    for (CheckpointParticipant participant : participants.values())
      participant.complete(record.id());
    store.complete(jobId, record);
    return record.completed();
  }

  private void validate(CheckpointRecord record) {
    if (!record.participants().keySet().equals(participants.keySet()))
      throw new IllegalArgumentException("Participant IDs must match stored checkpoint exactly");
  }

  private void abort(long id, Exception failure) {
    for (CheckpointParticipant participant : participants.values()) {
      try {
        participant.abort(id);
      } catch (Exception cleanup) {
        if (cleanup != failure) failure.addSuppressed(cleanup);
      }
    }
  }
}
