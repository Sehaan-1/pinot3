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
package org.apache.seatunnel.connector.fake;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.seatunnel.api.source.SourceReader.ReadStatus;
import org.apache.seatunnel.api.source.SplitEnumerator;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class FakeSourceTest {
  @Test
  void readerSnapshotRestoresOnlyUnconsumedRowsAndDoesNotAdvanceOnFailure() throws Exception {
    FakeSource source = new FakeSource(3, 10, 2);
    FakeSource.ReaderState snapshot;
    List<SeaTunnelRow> rows = new ArrayList<>();
    try (var reader = source.createReader(0, Optional.empty())) {
      assertThatThrownBy(() -> reader.pollNext(rows::add)).isInstanceOf(IllegalStateException.class);
      reader.start();
      reader.addSplits(List.of(new FakeSource.Split(10, 13)));
      reader.notifyNoMoreSplits();
      assertThatThrownBy(() -> reader.pollNext(row -> { throw new IOException("output failed"); }))
          .isInstanceOf(IOException.class);
      assertThat(reader.pollNext(rows::add)).isEqualTo(ReadStatus.MORE_AVAILABLE);
      snapshot = reader.snapshotState(1);
      reader.pollNext(rows::add);
      reader.notifyCheckpointComplete(1);
      reader.notifyCheckpointAborted(1);
      assertThat(snapshot.remaining()).containsExactly(new FakeSource.Split(11, 13));
      assertThatThrownBy(() -> reader.snapshotState(-1)).isInstanceOf(IllegalArgumentException.class);
    }
    rows.clear();
    try (var reader = source.createReader(0, Optional.of(snapshot))) {
      reader.start();
      assertThat(reader.pollNext(rows::add)).isEqualTo(ReadStatus.MORE_AVAILABLE);
      assertThat(reader.pollNext(rows::add)).isEqualTo(ReadStatus.END_OF_INPUT);
      assertThat(reader.pollNext(rows::add)).isEqualTo(ReadStatus.END_OF_INPUT);
      assertThat(rows).containsExactly(new SeaTunnelRow(11L, "row-11"), new SeaTunnelRow(12L, "row-12"));
    }
  }

  @Test
  void enumeratorRestoresAndRequeuesSplitsWithoutEagerlyMaterializingRanges() throws Exception {
    FakeSource source = new FakeSource(5, 10, 2);
    Context context = new Context();
    FakeSource.EnumeratorState snapshot;
    try (var enumerator = source.createEnumerator(context, Optional.empty())) {
      enumerator.start();
      enumerator.registerReader(0);
      enumerator.handleSplitRequest(0);
      assertThat(context.splits).containsExactly(new FakeSource.Split(10, 12));
      enumerator.addSplitsBack(List.copyOf(context.splits), 0);
      snapshot = enumerator.snapshotState(1);
      enumerator.handleSplitRequest(0);
      assertThat(snapshot.returned()).containsExactly(new FakeSource.Split(10, 12));
    }
    context.splits.clear();
    try (var restored = source.createEnumerator(context, Optional.of(snapshot))) {
      restored.start();
      restored.registerReader(0);
      for (int i = 0; i < 5; i++) {
        restored.handleSplitRequest(0);
      }
      assertThat(context.splits).containsExactly(new FakeSource.Split(10, 12),
          new FakeSource.Split(12, 14), new FakeSource.Split(14, 15));
      assertThat(context.completions).isEqualTo(1);
    }
  }

  @Test
  void rejectsBadConfigurationUnknownOptionsAndOutOfRangeRestore() {
    FakeSourcePlugin plugin = new FakeSourcePlugin();
    assertThat(plugin.identifier()).isEqualTo("fake");
    assertThatThrownBy(() -> plugin.createSource(Map.of("rows", -1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createSource(Map.of("split-size", 0)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createSource(Map.of("rows", "five")))
        .isInstanceOf(org.apache.seatunnel.common.exception.ConfigException.class);
    assertThatThrownBy(() -> plugin.createSource(Map.of("typo", 1)))
        .isInstanceOf(org.apache.seatunnel.common.exception.ConfigException.class);
    FakeSource source = new FakeSource(1, Integer.MAX_VALUE, 1);
    assertThatThrownBy(() -> source.createReader(0,
        Optional.of(new FakeSource.ReaderState(List.of(new FakeSource.Split(0, 1)), true))))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FakeSource.Split(1, 1)).isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void handlesLongIdsBeyondIntegerMaxAndClosedLifecycle() throws Exception {
    FakeSource source = new FakeSource(2, Integer.MAX_VALUE, 1);
    List<SeaTunnelRow> rows = new ArrayList<>();
    var reader = source.createReader(0, Optional.empty());
    reader.start();
    reader.addSplits(List.of(new FakeSource.Split(Integer.MAX_VALUE, (long) Integer.MAX_VALUE + 2)));
    reader.notifyNoMoreSplits();
    while (reader.pollNext(rows::add) != ReadStatus.END_OF_INPUT) { }
    assertThat(rows.get(1).getField(0)).isEqualTo((long) Integer.MAX_VALUE + 1);
    reader.close();
    reader.close();
    assertThatThrownBy(() -> reader.pollNext(rows::add)).isInstanceOf(IllegalStateException.class);
  }

  private static final class Context implements SplitEnumerator.Context<FakeSource.Split> {
    final List<FakeSource.Split> splits = new ArrayList<>();
    int completions;

    @Override
    public int currentParallelism() { return 1; }

    @Override
    public void assignSplits(int readerId, List<FakeSource.Split> assigned) { splits.addAll(assigned); }

    @Override
    public void signalNoMoreSplits(int readerId) { completions++; }
  }
}
