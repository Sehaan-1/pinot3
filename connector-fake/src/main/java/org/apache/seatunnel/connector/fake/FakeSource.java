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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SplitEnumerator;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Deterministic finite range source. Instances are configured once; operators are thread-confined. */
public final class FakeSource implements SeaTunnelSource<
    FakeSource.Split, FakeSource.ReaderState, FakeSource.EnumeratorState> {
  private final long start;
  private final long end;
  private final int splitSize;

  public FakeSource(int rows, int start, int splitSize) {
    if (rows < 0 || splitSize <= 0) {
      throw new IllegalArgumentException("rows must be nonnegative and split-size must be positive");
    }
    this.start = start;
    this.end = (long) start + rows;
    this.splitSize = splitSize;
  }

  public record Split(long start, long end) implements SourceSplit {
    public Split {
      if (start >= end) {
        throw new IllegalArgumentException("Split range must be nonempty");
      }
    }

    @Override
    public String splitId() {
      return start + ":" + end;
    }
  }

  public record ReaderState(List<Split> remaining, boolean noMoreSplits) implements CheckpointState {
    public ReaderState {
      remaining = List.copyOf(remaining);
    }
  }

  public record EnumeratorState(long next, List<Split> returned) implements CheckpointState {
    public EnumeratorState {
      returned = List.copyOf(returned);
    }
  }

  @Override
  public CatalogTable producedTable() {
    return new CatalogTable("fake", List.of(
        new Column("id", DataType.LONG, false), new Column("name", DataType.STRING, false)));
  }

  @Override
  public Boundedness boundedness() {
    return Boundedness.BOUNDED;
  }

  @Override
  public SourceReader<Split, ReaderState> createReader(int readerId, Optional<ReaderState> restored) {
    if (readerId < 0) {
      throw new IllegalArgumentException("readerId must be nonnegative");
    }
    ReaderState state = restored.orElseGet(() -> new ReaderState(List.of(), false));
    state.remaining().forEach(this::validateSplit);
    return new Reader(state);
  }

  @Override
  public SplitEnumerator<Split, EnumeratorState> createEnumerator(
      SplitEnumerator.Context<Split> context, Optional<EnumeratorState> restored) {
    EnumeratorState state = restored.orElseGet(() -> new EnumeratorState(start, List.of()));
    if (state.next() < start || state.next() > end) {
      throw new IllegalArgumentException("Enumerator state is outside the configured range");
    }
    state.returned().forEach(this::validateSplit);
    return new Enumerator(context, state);
  }

  private void validateSplit(Split split) {
    if (split.start() < start || split.end() > end) {
      throw new IllegalArgumentException("Split is outside the configured range");
    }
  }

  private abstract static class Lifecycle {
    private boolean started;
    private boolean closed;

    final void startOperator() {
      ensureOpen();
      if (started) {
        throw new IllegalStateException("Operator already started");
      }
      started = true;
    }

    final void ensureOpen() {
      if (closed) {
        throw new IllegalStateException("Operator is closed");
      }
    }

    final void ensureStarted() {
      ensureOpen();
      if (!started) {
        throw new IllegalStateException("Operator has not started");
      }
    }

    final void checkpoint(long id) {
      ensureStarted();
      if (id < 0) {
        throw new IllegalArgumentException("checkpoint ID must be nonnegative");
      }
    }

    public final void notifyCheckpointComplete(long id) {
      checkpoint(id); // Deterministic source: no external transaction to commit.
    }

    public final void notifyCheckpointAborted(long id) {
      checkpoint(id); // Snapshots are detached values, with no external resources.
    }

    public void close() {
      closed = true;
    }
  }

  private final class Reader extends Lifecycle implements SourceReader<Split, ReaderState> {
    private final Deque<Split> remaining;
    private boolean noMoreSplits;

    Reader(ReaderState state) {
      remaining = new ArrayDeque<>(state.remaining());
      noMoreSplits = state.noMoreSplits();
    }

    @Override
    public void start() {
      startOperator();
    }

    @Override
    public void addSplits(List<Split> splits) {
      ensureStarted();
      if (noMoreSplits) {
        throw new IllegalStateException("Splits already marked complete");
      }
      List<Split> copy = List.copyOf(splits);
      copy.forEach(FakeSource.this::validateSplit);
      remaining.addAll(copy);
    }

    @Override
    public void notifyNoMoreSplits() {
      ensureStarted();
      noMoreSplits = true;
    }

    @Override
    public ReadStatus pollNext(Output output) throws Exception {
      ensureStarted();
      Split split = remaining.peekFirst();
      if (split != null) {
        long id = split.start();
        output.collect(new SeaTunnelRow(id, "row-" + id));
        remaining.removeFirst(); // Advance only after output accepted the row.
        if (id + 1 < split.end()) {
          remaining.addFirst(new Split(id + 1, split.end()));
        }
      }
      return remaining.isEmpty() ? (noMoreSplits ? ReadStatus.END_OF_INPUT
          : ReadStatus.NOTHING_AVAILABLE) : ReadStatus.MORE_AVAILABLE;
    }

    @Override
    public ReaderState snapshotState(long id) {
      checkpoint(id);
      return new ReaderState(new ArrayList<>(remaining), noMoreSplits);
    }

    @Override
    public void close() {
      remaining.clear();
      super.close();
    }
  }

  private final class Enumerator extends Lifecycle implements SplitEnumerator<Split, EnumeratorState> {
    private final Context<Split> context;
    private final Deque<Split> returned;
    private final Set<Integer> registered = new HashSet<>();
    private final Set<Integer> completed = new HashSet<>();
    private long next;

    Enumerator(Context<Split> context, EnumeratorState state) {
      this.context = java.util.Objects.requireNonNull(context, "context");
      returned = new ArrayDeque<>(state.returned());
      next = state.next();
    }

    private void validateReader(int readerId) {
      ensureStarted();
      if (readerId < 0 || readerId >= context.currentParallelism()) {
        throw new IllegalArgumentException("Invalid reader ID: " + readerId);
      }
    }

    @Override
    public void start() {
      startOperator();
    }

    @Override
    public void registerReader(int readerId) {
      validateReader(readerId);
      registered.add(readerId);
    }

    @Override
    public void handleSplitRequest(int readerId) throws Exception {
      validateReader(readerId);
      if (!registered.contains(readerId)) {
        throw new IllegalStateException("Reader is not registered");
      }
      Split split = returned.peekFirst();
      if (split != null) {
        context.assignSplits(readerId, List.of(split));
        returned.removeFirst();
      } else if (next < end) {
        long limit = Math.min(end, next + splitSize);
        context.assignSplits(readerId, List.of(new Split(next, limit)));
        next = limit;
      } else if (!completed.contains(readerId)) {
        context.signalNoMoreSplits(readerId);
        completed.add(readerId);
      }
    }

    @Override
    public void addSplitsBack(List<Split> splits, int readerId) {
      validateReader(readerId);
      List<Split> copy = List.copyOf(splits);
      copy.forEach(FakeSource.this::validateSplit);
      returned.addAll(copy);
      completed.remove(readerId);
    }

    @Override
    public EnumeratorState snapshotState(long id) {
      checkpoint(id);
      return new EnumeratorState(next, new ArrayList<>(returned));
    }

    @Override
    public void close() {
      returned.clear();
      registered.clear();
      completed.clear();
      super.close();
    }
  }
}
