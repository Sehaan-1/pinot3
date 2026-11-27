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
package org.apache.seatunnel.engine.streaming;

import java.io.Serializable;
import java.util.*;
import java.util.function.LongSupplier;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.engine.checkpoint.*;

/** Cooperative, quiesced single-reader pump. No CLI streaming, automatic retries or HA claim. */
public final class StreamingRunner {
  private static final String PARTICIPANT = "pipeline";
  private final LongSupplier clock;

  public StreamingRunner() {
    this(System::nanoTime);
  }

  StreamingRunner(LongSupplier clock) {
    this.clock = Objects.requireNonNull(clock);
  }

  public record Settings(
      long checkpointMillis, int idleMillis, int maxEmissionsPerPoll, boolean requireCheckpoint)
      implements Serializable {
    public Settings(long checkpointMillis, int idleMillis, int maxEmissionsPerPoll) {
      this(checkpointMillis, idleMillis, maxEmissionsPerPoll, false);
    }

    public Settings {
      if (checkpointMillis < 1
          || checkpointMillis > 3600000
          || idleMillis < 1
          || idleMillis > 1000
          || maxEmissionsPerPoll < 1
          || maxEmissionsPerPoll > 10000)
        throw new IllegalArgumentException("Invalid streaming settings");
    }
  }

  /** Binding identity must change when connector config/semantics/state interpretation changes. */
  public record Pipeline<
      S extends SourceSplit,
      R extends CheckpointState,
      E extends CheckpointState,
      W extends CheckpointState,
      T extends Serializable>(
      String binding,
      SeaTunnelSource<S, R, E> source,
      SeaTunnelSink<W, T> sink,
      Class<R> readerState,
      Class<E> enumeratorState,
      Class<W> writerState,
      Class<T> handleType) {
    public Pipeline {
      if (Objects.requireNonNull(binding).isBlank() || binding.length() > 256)
        throw new IllegalArgumentException("Nonblank bounded binding required");
      Objects.requireNonNull(source);
      Objects.requireNonNull(sink);
      Objects.requireNonNull(readerState);
      Objects.requireNonNull(enumeratorState);
      Objects.requireNonNull(writerState);
      Objects.requireNonNull(handleType);
    }
  }

  /** Detached adapter states, not live operators/credentials. Store is trusted. */
  public record Snapshot(
      String binding,
      CatalogTable table,
      boolean transactional,
      CheckpointState reader,
      CheckpointState enumerator,
      CheckpointState writer,
      long rows,
      boolean finished)
      implements Serializable {
    public Snapshot {
      Objects.requireNonNull(binding);
      Objects.requireNonNull(table);
      Objects.requireNonNull(reader);
      Objects.requireNonNull(enumerator);
      Objects.requireNonNull(writer);
      if (rows < 0) throw new IllegalArgumentException("Negative row count");
    }
  }

  public <
          S extends SourceSplit,
          R extends CheckpointState,
          E extends CheckpointState,
          W extends CheckpointState,
          T extends Serializable>
      long run(
          UUID key,
          Pipeline<S, R, E, W, T> pipeline,
          CheckpointRepository repository,
          Settings settings)
          throws Exception {
    Objects.requireNonNull(key);
    Objects.requireNonNull(pipeline);
    Objects.requireNonNull(repository);
    Objects.requireNonNull(settings);
    interrupted();
    try (SinkCommitter<T> committer = pipeline.sink().createCommitter().orElse(null)) {
      var bridge = new Bridge<>(pipeline, committer);
      CheckpointRecord before = repository.get(key);
      if (before == null && settings.requireCheckpoint())
        throw new IllegalStateException(
            "Required streaming checkpoint is absent; refusing fresh start");
      if (before != null) bridge.validate(before);
      var coordinator = new CheckpointCoordinator(key, repository, Map.of(PARTICIPANT, bridge));
      boolean pendingRecovery =
          before != null && before.status() == CheckpointRecord.Status.COMMITTING;
      CheckpointRecord restored;
      if (pendingRecovery) {
        bridge.restore(before.participants().get(PARTICIPANT).state());
        try {
          coordinator.recover();
        } catch (OperatorsRequired expected) {
          /* Commit acked, notifications need restored operators. */
        }
        restored = before;
      } else {
        coordinator.recover();
        restored = coordinator.restore();
      }
      if (!pendingRecovery && bridge.restored != null && bridge.restored.finished())
        return bridge.restored.rows();
      long id = restored == null ? 0 : restored.id();
      try (SourceReader<S, R> reader = pipeline.source().createReader(0, bridge.readerState());
          SinkWriter<W, T> writer = pipeline.sink().createWriter(0, bridge.writerState())) {
        bridge.reader = reader;
        bridge.writer = writer;
        SplitEnumerator.Context<S> context =
            new SplitEnumerator.Context<>() {
              public int currentParallelism() {
                return 1;
              }

              public void assignSplits(int readerId, List<S> splits) throws Exception {
                if (readerId != 0) throw new IllegalArgumentException("Unknown streaming reader");
                reader.addSplits(List.copyOf(splits));
              }

              public void signalNoMoreSplits(int readerId) throws Exception {
                if (readerId != 0) throw new IllegalArgumentException("Unknown streaming reader");
                reader.notifyNoMoreSplits();
              }
            };
        try (SplitEnumerator<S, E> enumerator =
            pipeline.source().createEnumerator(context, bridge.enumeratorState())) {
          bridge.enumerator = enumerator;
          reader.start();
          enumerator.start();
          if (pendingRecovery) {
            // Idempotent commit replay + live notifications before COMPLETED.
            coordinator.recover();
            coordinator.restore();
          } else if (restored != null) bridge.complete(restored.id());
          if (bridge.finished) return bridge.rows;
          if (id == Long.MAX_VALUE) throw new IllegalStateException("Checkpoint IDs exhausted");
          enumerator.registerReader(0);
          enumerator.handleSplitRequest(0);
          long last = clock.getAsLong();
          int arity = pipeline.source().producedTable().columns().size();
          while (true) {
            interrupted();
            int[] emissions = {0};
            SourceReader.ReadStatus status =
                Objects.requireNonNull(
                    reader.pollNext(
                        row -> {
                          interrupted();
                          if (++emissions[0] > settings.maxEmissionsPerPoll())
                            throw new IllegalStateException("Streaming poll emission cap exceeded");
                          if (Objects.requireNonNull(row).getArity() != arity)
                            throw new IllegalArgumentException("Streaming source arity mismatch");
                          writer.write(row);
                          bridge.rows = Math.incrementExact(bridge.rows);
                        }),
                    "read status");
            interrupted();
            long now = clock.getAsLong();
            if (status == SourceReader.ReadStatus.END_OF_INPUT
                || now - last >= settings.checkpointMillis() * 1000000L) {
              bridge.finished = status == SourceReader.ReadStatus.END_OF_INPUT;
              coordinator.checkpoint(Math.incrementExact(id));
              id++;
              last = clock.getAsLong();
              if (bridge.finished) return bridge.rows;
              if (id == Long.MAX_VALUE) throw new IllegalStateException("Checkpoint IDs exhausted");
            }
            if (status == SourceReader.ReadStatus.NOTHING_AVAILABLE) {
              enumerator.handleSplitRequest(0);
              Thread.sleep(settings.idleMillis());
            }
          }
        }
      }
    } catch (InterruptedException cancelled) {
      Thread.currentThread().interrupt();
      throw cancelled;
    }
  }

  private static final class OperatorsRequired extends Exception {}

  private static void interrupted() throws InterruptedException {
    if (Thread.currentThread().isInterrupted())
      throw new InterruptedException("Streaming pipeline interrupted");
  }

  private static final class Bridge<
          S extends SourceSplit,
          R extends CheckpointState,
          E extends CheckpointState,
          W extends CheckpointState,
          T extends Serializable>
      implements CheckpointParticipant {
    final Pipeline<S, R, E, W, T> pipeline;
    final SinkCommitter<T> committer;
    SourceReader<S, R> reader;
    SplitEnumerator<S, E> enumerator;
    SinkWriter<W, T> writer;
    Snapshot restored;
    long rows;
    boolean finished;
    List<Serializable> pending = List.of();

    Bridge(Pipeline<S, R, E, W, T> pipeline, SinkCommitter<T> committer) {
      this.pipeline = pipeline;
      this.committer = committer;
    }

    void validate(CheckpointRecord record) {
      if (!record.participants().keySet().equals(Set.of(PARTICIPANT)))
        throw new IllegalArgumentException("Streaming checkpoint participant mismatch");
      var prepared = record.participants().get(PARTICIPANT);
      checked(prepared.state());
      handles(prepared.handles());
    }

    Snapshot checked(Serializable state) {
      if (!(state instanceof Snapshot snapshot)
          || !snapshot.binding().equals(pipeline.binding())
          || !snapshot.table().equals(pipeline.source().producedTable())
          || snapshot.transactional() != (committer != null)
          || !pipeline.readerState().isInstance(snapshot.reader())
          || !pipeline.enumeratorState().isInstance(snapshot.enumerator())
          || !pipeline.writerState().isInstance(snapshot.writer()))
        throw new IllegalArgumentException("Streaming checkpoint binding/schema/state mismatch");
      return snapshot;
    }

    List<T> handles(List<Serializable> handles) {
      if (committer == null && !handles.isEmpty())
        throw new IllegalArgumentException("Nontransactional sink returned handles");
      return handles.stream().map(pipeline.handleType()::cast).toList();
    }

    Optional<R> readerState() {
      return restored == null
          ? Optional.empty()
          : Optional.of(pipeline.readerState().cast(restored.reader()));
    }

    Optional<E> enumeratorState() {
      return restored == null
          ? Optional.empty()
          : Optional.of(pipeline.enumeratorState().cast(restored.enumerator()));
    }

    Optional<W> writerState() {
      return restored == null
          ? Optional.empty()
          : Optional.of(pipeline.writerState().cast(restored.writer()));
    }

    public PreparedState prepare(long id) throws Exception {
      pending = List.of();
      writer.flush(finished);
      pending = List.copyOf(writer.prepareCommit(id));
      handles(pending);
      W sinkState = writer.snapshotState(id);
      E enumState = enumerator.snapshotState(id);
      R sourceState = reader.snapshotState(id);
      var state =
          new Snapshot(
              pipeline.binding(),
              pipeline.source().producedTable(),
              committer != null,
              sourceState,
              enumState,
              sinkState,
              rows,
              finished);
      checked(state);
      return new PreparedState(state, pending);
    }

    public void commit(long id, List<Serializable> handles) throws Exception {
      var typed = handles(handles);
      if (committer != null) committer.commit(id, typed);
    }

    public void complete(long id) throws Exception {
      // During pending-decision recovery there are no live operators yet; notify after
      // restore/start.
      if (writer == null) throw new OperatorsRequired();
      writer.notifyCheckpointComplete(id);
      enumerator.notifyCheckpointComplete(id);
      reader.notifyCheckpointComplete(id);
    }

    public void abort(long id) throws Exception {
      Exception failure = null;
      if (committer != null) {
        try {
          committer.abort(id, handles(pending));
        } catch (Exception e) {
          failure = e;
        }
      }
      for (var operator : List.of(writer, enumerator, reader)) {
        try {
          operator.notifyCheckpointAborted(id);
        } catch (Exception e) {
          if (failure == null) failure = e;
          else if (failure != e) failure.addSuppressed(e);
        }
      }
      if (failure != null) throw failure;
    }

    public void restore(Serializable state) {
      restored = checked(state);
      rows = restored.rows();
      finished = restored.finished();
    }
  }
}
