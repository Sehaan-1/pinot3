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
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.seatunnel.api.source.SourceSplit;
import org.junit.jupiter.api.Test;

class SplitLedgerTest {
  private record Split(String splitId) implements SourceSplit {}
  @Test void assignsRequeuesAndRestoresUnfinishedWork() {
    var ledger = new SplitLedger<>(2, List.of(new Split("a"), new Split("b"), new Split("c")));
    assertThat(ledger.assign(0, 2)).extracting(Split::splitId).containsExactly("a", "b");
    assertThat(ledger.assign(1, 2)).extracting(Split::splitId).containsExactly("c");
    assertThat(ledger.assign(1, 1)).isEmpty();
    assertThatThrownBy(() -> ledger.complete(1, "a")).isInstanceOf(IllegalArgumentException.class);
    ledger.complete(0, "a");
    var snapshot = ledger.snapshot();
    assertThatThrownBy(() -> snapshot.assigned().put("x", 0)).isInstanceOf(UnsupportedOperationException.class);
    ledger.readerFailed(0);
    assertThat(ledger.assign(1, 10)).extracting(Split::splitId).containsExactly("b");
    var restored = SplitLedger.restore(2, snapshot);
    assertThat(restored.assign(0, 10)).extracting(Split::splitId).containsExactly("b", "c");
    assertThat(restored.snapshot().completed()).containsExactly("a");
  }
  @Test void validatesInputsAndSnapshots() {
    assertThatThrownBy(() -> new SplitLedger<>(0, List.of())).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SplitLedger<>(1, List.of(new Split("x"), new Split("x")))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SplitLedger<>(1, List.of(new Split(" ")))).isInstanceOf(IllegalArgumentException.class);
    var ledger = new SplitLedger<Split>(1, List.of());
    assertThatThrownBy(() -> ledger.assign(1, 1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.assign(0, 0)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> ledger.readerFailed(-1)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SplitLedger.restore(1, new SplitLedger.Snapshot<>(List.of(new Split("a")), Map.of("a", 0), Set.of("a")))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SplitLedger.restore(1, new SplitLedger.Snapshot<>(List.of(), Map.of(), Set.of("unknown")))).isInstanceOf(IllegalArgumentException.class);
  }
  @Test void validatesDecisionRecordsAndDetachesCollections() {
    var handles = new java.util.ArrayList<java.io.Serializable>(); handles.add("tx");
    var state = new PreparedState("offset", handles); handles.clear();
    assertThat(state.handles()).containsExactly("tx");
    assertThatThrownBy(() -> new PreparedState(null, List.of())).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new CheckpointRecord(0, CheckpointRecord.Status.COMPLETED, Map.of("p",state),null)).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CheckpointRecord(1, CheckpointRecord.Status.COMPLETED, Map.of(),null)).isInstanceOf(IllegalArgumentException.class);
    var done = new CheckpointRecord(1, CheckpointRecord.Status.COMPLETED, Map.of("p",state),null);
    assertThatThrownBy(() -> new CheckpointRecord(2, CheckpointRecord.Status.COMPLETED, Map.of("p",state),done)).isInstanceOf(IllegalArgumentException.class);
    assertThat(new CheckpointRecord(2, CheckpointRecord.Status.COMMITTING, Map.of("p",state),done).completed().previous()).isNull();
  }
}
