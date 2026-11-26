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
package org.apache.seatunnel.api;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

/** Test-local reference implementation checks generic SPI composition, not an execution engine. */
class CheckpointContractTest {
  record State(List<SeaTunnelRow> rows) implements CheckpointState {
    State {
      rows = List.copyOf(rows);
    }
  }

  @Test
  void snapshotRestoreAndCommitCanBeComposedWithoutEngineTypes() throws Exception {
    ReferenceSink sink = new ReferenceSink();
    State snapshot;
    try (SinkWriter<State, String> writer = sink.createWriter(0, Optional.empty())) {
      writer.write(new SeaTunnelRow(1));
      snapshot = writer.snapshotState(7);
      writer.write(new SeaTunnelRow(2));
      assertThat(snapshot.rows()).containsExactly(new SeaTunnelRow(1));
      writer.notifyCheckpointAborted(7);
    }
    try (SinkWriter<State, String> writer = sink.createWriter(0, Optional.of(snapshot));
        SinkCommitter<String> committer = sink.createCommitter().orElseThrow()) {
      writer.flush(true);
      List<String> handles = writer.prepareCommit(8);
      committer.commit(8, handles);
      committer.commit(8, handles);
      writer.notifyCheckpointComplete(8);
      assertThat(sink.committed).containsExactly("8:1");
      committer.abort(9, List.of("9:unused"));
      assertThat(sink.committed).containsExactly("8:1");
    }
  }

  private static final class ReferenceSink implements SeaTunnelSink<State, String> {
    private final Set<String> committed = new HashSet<>();

    @Override
    public SinkWriter<State, String> createWriter(int writerId, Optional<State> restoredState) {
      return new Writer(restoredState.orElseGet(() -> new State(List.of())));
    }

    @Override
    public Optional<SinkCommitter<String>> createCommitter() {
      return Optional.of(new SinkCommitter<>() {
        @Override
        public void commit(long checkpointId, List<String> handles) {
          committed.addAll(handles);
        }

        @Override
        public void abort(long checkpointId, List<String> handles) {
          committed.removeAll(handles);
        }

        @Override
        public void close() {
          // This in-memory test committer holds no external resources.
        }
      });
    }
  }

  private static final class Writer implements SinkWriter<State, String> {
    private final List<SeaTunnelRow> rows;
    private final Set<Long> completed = new HashSet<>();
    private boolean flushed;

    Writer(State state) {
      rows = new ArrayList<>(state.rows());
    }

    @Override
    public void write(SeaTunnelRow row) {
      rows.add(row);
      flushed = false;
    }

    @Override
    public void flush(boolean endOfInput) {
      flushed = true;
    }

    @Override
    public List<String> prepareCommit(long checkpointId) {
      if (!flushed) {
        throw new IllegalStateException("Flush before preparing commit");
      }
      return List.of(checkpointId + ":" + rows.size());
    }

    @Override
    public State snapshotState(long checkpointId) {
      return new State(rows);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
      completed.add(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
      completed.remove(checkpointId);
    }

    @Override
    public void close() {
      rows.clear();
    }
  }
}
