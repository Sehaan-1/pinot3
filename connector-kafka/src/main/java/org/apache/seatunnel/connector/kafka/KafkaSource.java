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
package org.apache.seatunnel.connector.kafka;

import java.util.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.*;

/**
 * Manual partition assignment, accepted-offset checkpoints, finite ranges or cooperative live read.
 */
public final class KafkaSource
    implements SeaTunnelSource<
        KafkaSource.Split, KafkaSource.ReaderState, KafkaSource.EnumeratorState> {
  final KafkaConfig config;
  private final KafkaClients.ReaderFactory clients;

  KafkaSource(KafkaConfig config) {
    this(config, KafkaSdk::reader);
  }

  KafkaSource(KafkaConfig config, KafkaClients.ReaderFactory clients) {
    this.config = config;
    this.clients = clients;
  }

  public record Split(String topic, int partition, long start, Long end) implements SourceSplit {
    public Split {
      Objects.requireNonNull(topic);
      if (partition < 0 || start < 0 || (end != null && end < start))
        throw KafkaConfig.bad("Invalid Kafka offset range");
    }

    public String splitId() {
      return topic + ":" + partition;
    }
  }

  public record ReaderState(
      String group,
      List<Split> splits,
      Map<Integer, Long> offsets,
      boolean assigned,
      long checkpointId)
      implements CheckpointState {
    public ReaderState {
      Objects.requireNonNull(group);
      splits = List.copyOf(splits);
      offsets = Map.copyOf(offsets);
      if (checkpointId < 0) throw new IllegalArgumentException("Invalid checkpoint id");
    }
  }

  public record EnumeratorState(List<Split> splits, boolean assigned) implements CheckpointState {
    public EnumeratorState {
      splits = List.copyOf(splits);
    }
  }

  public CatalogTable producedTable() {
    return KafkaConfig.TABLE;
  }

  public Boundedness boundedness() {
    return config.mode.equals("bounded") ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
  }

  public SourceReader<Split, ReaderState> createReader(int id, Optional<ReaderState> restored) {
    if (id != 0) throw new IllegalArgumentException("Kafka baseline requires reader0");
    restored.ifPresent(this::validate);
    return new Reader(restored.orElse(null));
  }

  void validate(ReaderState state) {
    if (!config.group.equals(state.group())
        || !config.splits.equals(state.splits())
        || state.checkpointId() == 0)
      throw new IllegalArgumentException("Kafka source state binding mismatch");
    var expected = new HashSet<Integer>();
    for (Split split : config.splits) {
      expected.add(split.partition());
      Long next = state.offsets().get(split.partition());
      if (next == null
          || next < split.start()
          || (split.end() != null && next > split.end())
          || (!state.assigned() && next != split.start()))
        throw new IllegalArgumentException("Invalid Kafka restored offset");
    }
    if (!expected.equals(state.offsets().keySet()))
      throw new IllegalArgumentException("Kafka partition set mismatch");
  }

  public SplitEnumerator<Split, EnumeratorState> createEnumerator(
      SplitEnumerator.Context<Split> context, Optional<EnumeratorState> restored) {
    Objects.requireNonNull(context);
    if (context.currentParallelism() != 1)
      throw new IllegalArgumentException("Kafka baseline requires parallelism1");
    restored.ifPresent(
        s -> {
          if (!config.splits.equals(s.splits()))
            throw new IllegalArgumentException("Kafka enum binding mismatch");
        });
    return new SplitEnumerator<>() {
      boolean started,
          closed,
          assigned = restored.map(EnumeratorState::assigned).orElse(false),
          registered;

      private void active() {
        if (!started || closed) throw new IllegalStateException("Kafka enumerator inactive");
      }

      private void check(int id) {
        active();
        if (id != 0) throw new IllegalArgumentException("Unknown Kafka reader");
      }

      public void start() {
        if (started || closed) throw new IllegalStateException("Cannot restart enumerator");
        started = true;
      }

      public void registerReader(int id) {
        check(id);
        registered = true;
      }

      public void handleSplitRequest(int id) throws Exception {
        check(id);
        if (!registered) throw new IllegalStateException("Reader not registered");
        if (!assigned) {
          context.assignSplits(0, config.splits);
          assigned = true;
        }
        context.signalNoMoreSplits(0);
      }

      public void addSplitsBack(List<Split> splits, int id) {
        throw new UnsupportedOperationException("Restart Kafka from checkpoint instead");
      }

      public EnumeratorState snapshotState(long id) {
        active();
        if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
        return new EnumeratorState(config.splits, assigned);
      }

      public void notifyCheckpointComplete(long id) {
        active();
        if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
      }

      public void notifyCheckpointAborted(long id) {
        notifyCheckpointComplete(id);
      }

      public void close() {
        closed = true;
      }
    };
  }

  private final class Reader implements SourceReader<Split, ReaderState> {
    final Map<Integer, Long> offsets = new HashMap<>();
    final Map<Integer, Split> splits = new HashMap<>();
    KafkaClients.Reader client;
    boolean started, closed, assigned;
    Iterator<KafkaClients.Message> buffered = Collections.emptyIterator();
    ReaderState pending;
    long lastAttempt, lastCompleted;

    Reader(ReaderState state) {
      for (Split split : config.splits) {
        splits.put(split.partition(), split);
        offsets.put(
            split.partition(),
            state == null ? split.start() : state.offsets().get(split.partition()));
      }
      if (state != null) {
        assigned = state.assigned();
        pending = state;
        lastAttempt = state.checkpointId();
      }
    }

    void active() {
      if (!started || closed) throw new IllegalStateException("Kafka reader inactive");
    }

    void checkInterrupt() throws InterruptedException {
      if (Thread.currentThread().isInterrupted())
        throw new InterruptedException("Kafka reader interrupted");
    }

    public void start() throws Exception {
      if (started || closed) throw new IllegalStateException("Cannot restart Kafka reader");
      started = true;
      try {
        client = clients.open(config);
        if (assigned) assign();
      } catch (Exception e) {
        close();
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Kafka client interrupted");
        throw KafkaConfig.failure();
      }
    }

    void assign() {
      client.assign(config.splits);
      var begin = client.beginnings();
      var end = client.ends();
      for (Split split : config.splits) {
        Long first = begin.get(split.partition()), limit = end.get(split.partition());
        long next = offsets.get(split.partition());
        if (first == null
            || limit == null
            || next < first
            || next > limit
            || (split.end() != null && split.end() > limit)) throw KafkaConfig.failure();
        client.seek(split.partition(), next);
      }
      pause();
    }

    void pause() {
      var done = new HashSet<Integer>();
      for (Split split : config.splits)
        if (split.end() != null && offsets.get(split.partition()) >= split.end())
          done.add(split.partition());
      if (!done.isEmpty()) client.pause(done);
    }

    public void addSplits(List<Split> values) throws Exception {
      active();
      if (assigned || !config.splits.equals(List.copyOf(values)))
        throw new IllegalArgumentException("Foreign/duplicate Kafka assignment");
      try {
        assign();
        assigned = true;
      } catch (Exception e) {
        close();
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Kafka client interrupted");
        throw KafkaConfig.failure();
      }
    }

    public void notifyNoMoreSplits() {
      active();
      if (!assigned) throw new IllegalStateException("Kafka assignment required");
    }

    boolean complete() {
      return config.mode.equals("bounded")
          && config.splits.stream().allMatch(s -> offsets.get(s.partition()) >= s.end());
    }

    void reconcile() {
      for (Split split : config.splits) {
        long position = client.position(split.partition());
        long next = split.end() == null ? position : Math.min(position, split.end());
        if (next < offsets.get(split.partition())) throw KafkaConfig.failure();
        offsets.put(split.partition(), next);
      }
      pause();
    }

    public ReadStatus pollNext(Output output) throws Exception {
      active();
      if (!assigned) return ReadStatus.NOTHING_AVAILABLE;
      try {
        checkInterrupt();
        if (complete()) return ReadStatus.END_OF_INPUT;
        if (!buffered.hasNext()) {
          var batch = client.poll();
          if (batch.size() > config.maxPoll) throw KafkaConfig.failure();
          buffered = batch.iterator();
        }
        while (buffered.hasNext()) {
          var record = buffered.next();
          Split split = splits.get(record.partition());
          if (split == null || !config.topic.equals(record.topic()) || record.offset() < 0)
            throw KafkaConfig.failure();
          if (record.offset() < offsets.get(record.partition())
              || (split.end() != null && record.offset() >= split.end())) continue;
          long next = Math.incrementExact(record.offset());
          if ((long) (record.key() == null ? 0 : record.key().length)
                  + (record.value() == null ? 0 : record.value().length)
              > config.maxBytes) throw KafkaConfig.failure();
          checkInterrupt();
          output.collect(
              new SeaTunnelRow(
                  record.topic(),
                  record.partition(),
                  record.offset(),
                  record.key() == null ? null : record.key().clone(),
                  record.value() == null ? null : record.value().clone()));
          offsets.put(record.partition(), next);
          if (!buffered.hasNext()) reconcile();
          return complete() ? ReadStatus.END_OF_INPUT : ReadStatus.MORE_AVAILABLE;
        }
        reconcile();
        return complete() ? ReadStatus.END_OF_INPUT : ReadStatus.NOTHING_AVAILABLE;
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Kafka client interrupted");
        throw KafkaConfig.failure();
      }
    }

    public ReaderState snapshotState(long id) {
      active();
      if (id <= lastAttempt || pending != null)
        throw new IllegalStateException("Kafka checkpoint must advance without overlap");
      lastAttempt = id;
      pending = new ReaderState(config.group, config.splits, offsets, assigned, id);
      return pending;
    }

    public void notifyCheckpointComplete(long id) throws Exception {
      active();
      if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
      if (id <= lastCompleted) return;
      if (pending == null || pending.checkpointId() != id)
        throw new IllegalStateException("Kafka completion requires saved offsets");
      try {
        if (assigned) client.commit(pending.offsets());
        lastCompleted = id;
        pending = null;
      } catch (Exception e) {
        close();
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Kafka client interrupted");
        throw KafkaConfig.failure();
      }
    }

    public void notifyCheckpointAborted(long id) {
      active();
      if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
      if (pending != null && pending.checkpointId() == id) pending = null;
    }

    public void close() {
      if (closed) return;
      closed = true;
      buffered = Collections.emptyIterator();
      if (client != null)
        try {
          client.close();
        } catch (Exception ignored) {
          /* Do not leak client diagnostics. */
        }
    }
  }
}
