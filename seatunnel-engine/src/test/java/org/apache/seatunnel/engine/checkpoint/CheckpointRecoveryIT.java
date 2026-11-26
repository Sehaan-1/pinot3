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

import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;

import java.io.Serializable;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.seatunnel.engine.cluster.EngineNode;
import org.apache.seatunnel.engine.cluster.EngineSettings;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Real replicated store and replacement coordinator; no mocked decision or commit paths. */
class CheckpointRecoveryIT {
  private static EngineNode first;
  private static EngineNode second;

  @BeforeAll
  static void start() throws Exception {
    String cluster = "checkpoint-it-" + UUID.randomUUID();
    first = EngineNode.start(settings(cluster, List.of()));
    try {
      second =
          EngineNode.start(
              settings(
                  cluster,
                  List.of(
                      "127.0.0.1:"
                          + first.member().getCluster().getLocalMember().getAddress().getPort())));
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> {
                assertThat(first.member().getCluster().getMembers()).hasSize(2);
                assertThat(second.member().getCluster().getMembers()).hasSize(2);
                assertThat(first.member().getPartitionService().isClusterSafe()).isTrue();
              });
    } catch (Throwable failure) {
      stop();
      throw failure;
    }
  }

  private static EngineSettings settings(String cluster, List<String> seeds) {
    return new EngineSettings(cluster, "127.0.0.1", 0, seeds, 2, 16, 2, Duration.ofSeconds(5));
  }

  @AfterAll
  static void stop() throws Exception {
    try {
      if (second != null) second.close();
    } finally {
      if (first != null) first.close();
    }
  }

  @Test
  void retriesCommitFromOtherMemberThenRestoresSnapshot() throws Exception {
    UUID job = UUID.randomUUID();
    var store = new CheckpointStore(first.member());
    var original = new Participant(store, job);
    var coordinator = coordinator(job, store, original);
    assertThat(coordinator.restore()).isNull();
    assertThat(coordinator.recover()).isNull();
    coordinator.checkpoint(1);
    original.failCommit = true;
    assertThatThrownBy(() -> coordinator.checkpoint(2)).hasMessage("commit failed");
    assertThat(original.aborts).isZero();
    assertThat(store.get(job).status()).isEqualTo(CheckpointRecord.Status.COMMITTING);
    assertThat(store.get(job).previous().id()).isEqualTo(1);
    assertThatThrownBy(() -> coordinator.checkpoint(3)).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(coordinator::restore).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> store.delete(job)).isInstanceOf(IllegalStateException.class);
    var otherStore = new CheckpointStore(second.member());
    var replacement = new Participant(otherStore, job);
    var recovered = coordinator(job, otherStore, replacement);
    assertThat(recovered.recover().status()).isEqualTo(CheckpointRecord.Status.COMPLETED);
    assertThat(replacement.commits).isEqualTo(1);
    assertThat(replacement.handles).containsExactly("transaction-2");
    assertThat(recovered.recover().id()).isEqualTo(2);
    assertThat(replacement.commits).isEqualTo(1);
    recovered.restore();
    assertThat(replacement.restored).isEqualTo("offset-2");
    assertThat(otherStore.get(job).previous()).isNull();
    assertThatThrownBy(() -> recovered.checkpoint(2)).isInstanceOf(IllegalArgumentException.class);
    var mismatch = new CheckpointCoordinator(job, otherStore, Map.of("other", replacement));
    assertThatThrownBy(mismatch::restore).isInstanceOf(IllegalArgumentException.class);
    otherStore.delete(job);
    assertThat(store.get(job)).isNull();
    otherStore.delete(job);
  }

  @Test
  void abortsFailedPreparationButNeverAbortsPublishedCommit() throws Exception {
    UUID job = UUID.randomUUID();
    var store = new CheckpointStore(first.member());
    var participant = new Participant(store, job);
    var coordinator = coordinator(job, store, participant);
    participant.failPrepare = true;
    participant.failAbort = true;
    assertThatThrownBy(() -> coordinator.checkpoint(1))
        .hasMessage("prepare failed")
        .satisfies(e -> assertThat(e.getSuppressed()).hasSize(1));
    assertThat(participant.aborts).isEqualTo(1);
    assertThat(store.get(job)).isNull();
    participant.failPrepare = false;
    participant.failAbort = false;
    participant.failComplete = true;
    assertThatThrownBy(() -> coordinator.checkpoint(2)).hasMessage("complete failed");
    assertThat(store.get(job).status()).isEqualTo(CheckpointRecord.Status.COMMITTING);
    assertThat(participant.aborts).isEqualTo(1);
    participant.failComplete = false;
    coordinator.recover();
    assertThat(participant.commits).isEqualTo(2);
    store.delete(job);
  }

  @Test
  void retainsReplicatedDecisionAcrossMemberDeparture() throws Exception {
    // Use a third temporary member so other tests keep their two-member fixture.
    String cluster = first.member().getConfig().getClusterName();
    EngineNode temporary =
        EngineNode.start(
            settings(
                cluster,
                List.of(
                    "127.0.0.1:"
                        + first.member().getCluster().getLocalMember().getAddress().getPort())));
    UUID job = UUID.randomUUID();
    try {
      await()
          .atMost(Duration.ofSeconds(20))
          .untilAsserted(
              () -> assertThat(temporary.member().getPartitionService().isClusterSafe()).isTrue());
      var store = new CheckpointStore(temporary.member());
      var p = new Participant(store, job);
      p.failCommit = true;
      assertThatThrownBy(() -> coordinator(job, store, p).checkpoint(1))
          .hasMessage("commit failed");
    } finally {
      temporary.close();
    }
    var surviving = new CheckpointStore(second.member());
    var p = new Participant(surviving, job);
    assertThat(coordinator(job, surviving, p).recover().status())
        .isEqualTo(CheckpointRecord.Status.COMPLETED);
    assertThat(p.handles).containsExactly("transaction-1");
    surviving.delete(job);
  }

  @Test
  void checkpointsSplitOwnershipAndRestoresUnfinishedAssignments() throws Exception {
    UUID job = UUID.randomUUID();
    var ledger = new SplitLedger<>(2, List.of(new Split("a"), new Split("b")));
    ledger.assign(0, 2);
    ledger.complete(0, "a");
    var store = new CheckpointStore(first.member());
    var participant = new Participant(store, job);
    participant.snapshot = ledger.snapshot();
    coordinator(job, store, participant).checkpoint(1);
    var other = new CheckpointStore(second.member());
    var replacement = new Participant(other, job);
    coordinator(job, other, replacement).restore();
    assertThat(replacement.restored).isInstanceOf(SplitLedger.Snapshot.class);
    @SuppressWarnings("unchecked")
    var snapshot = (SplitLedger.Snapshot<Split>) replacement.restored;
    assertThat(SplitLedger.restore(2, snapshot).assign(1, 10))
        .extracting(Split::splitId).containsExactly("b");
    other.delete(job);
  }

  private record Split(String splitId) implements org.apache.seatunnel.api.source.SourceSplit {}

  @Test
  void coordinatesAllParticipantsWithoutPayloadEquality() throws Exception {
    UUID job = UUID.randomUUID();
    var store = new CheckpointStore(first.member());
    var left = new Participant(store, job);
    var right = new Participant(store, job);
    left.snapshot = new Offset(10);
    right.snapshot = new Offset(20);
    var coordinator = new CheckpointCoordinator(job, store, Map.of("left", left, "right", right));
    coordinator.checkpoint(1);
    var other = new CheckpointStore(second.member());
    var replacement = new CheckpointCoordinator(job, other, Map.of("left", left, "right", right));
    replacement.checkpoint(2);
    replacement.restore();
    assertThat(((Offset) left.restored).value).isEqualTo(10);
    assertThat(((Offset) right.restored).value).isEqualTo(20);
    assertThat(left.commits).isEqualTo(2);
    assertThat(right.commits).isEqualTo(2);
    other.delete(job);
  }

  private static final class Offset implements Serializable {
    final int value;
    Offset(int value) { this.value = value; }
  }

  private static CheckpointCoordinator coordinator(UUID job, CheckpointStore store, Participant p) {
    return new CheckpointCoordinator(job, store, Map.of("operator", p));
  }

  private static final class Participant implements CheckpointParticipant {
    final CheckpointStore store;
    final UUID job;
    boolean failPrepare, failCommit, failComplete, failAbort;
    int commits, aborts;
    Serializable restored;
    Serializable snapshot;
    List<Serializable> handles;

    Participant(CheckpointStore store, UUID job) {
      this.store = store;
      this.job = job;
    }

    public PreparedState prepare(long id) throws Exception {
      if (failPrepare) throw new Exception("prepare failed");
      return new PreparedState(snapshot == null ? "offset-" + id : snapshot, List.of("transaction-" + id));
    }

    public void commit(long id, List<Serializable> handles) throws Exception {
      assertThat(store.get(job).status()).isEqualTo(CheckpointRecord.Status.COMMITTING);
      this.handles = handles;
      commits++;
      if (failCommit) throw new Exception("commit failed");
    }

    public void complete(long id) throws Exception {
      if (failComplete) throw new Exception("complete failed");
    }

    public void abort(long id) throws Exception {
      aborts++;
      if (failAbort) throw new Exception("abort failed");
    }

    public void restore(Serializable state) {
      restored = state;
    }
  }
}
