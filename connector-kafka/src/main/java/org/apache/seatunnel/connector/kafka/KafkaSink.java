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
import java.util.concurrent.*;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.*;

/** SDK-acknowledged, INSERT-only byte records. Session idempotence is not exactly-once restore. */
public final class KafkaSink implements SeaTunnelSink<KafkaSink.State, String> {
  final KafkaConfig config;
  private final KafkaClients.WriterFactory clients;

  KafkaSink(KafkaConfig config) {
    this(config, KafkaSdk::writer);
  }

  KafkaSink(KafkaConfig config, KafkaClients.WriterFactory clients) {
    this.config = config;
    this.clients = clients;
  }

  public record State(long acknowledged) implements CheckpointState {
    public State {
      if (acknowledged < 0) throw new IllegalArgumentException("Negative Kafka ack count");
    }
  }

  public Optional<SinkCommitter<String>> createCommitter() {
    return Optional.empty();
  }

  public SinkWriter<State, String> createWriter(int id, Optional<State> restored) {
    if (id != 0) throw new IllegalArgumentException("Kafka requires writer0");
    return new Writer(restored.map(State::acknowledged).orElse(0L));
  }

  private final class Writer implements SinkWriter<State, String> {
    KafkaClients.Writer client;
    boolean closed;
    long acknowledged;
    final Deque<Future<?>> pending = new ArrayDeque<>();

    Writer(long restored) {
      acknowledged = restored;
      try {
        client = clients.open(config);
      } catch (Exception e) {
        close();
        throw KafkaConfig.failure();
      }
    }

    void active() {
      if (closed) throw new IllegalStateException("Kafka writer closed");
    }

    void await() throws Exception {
      pending.peekFirst().get(config.timeout, TimeUnit.MILLISECONDS);
      pending.removeFirst();
      acknowledged = Math.incrementExact(acknowledged);
    }

    public void write(SeaTunnelRow row) throws Exception {
      active();
      try {
        if (Thread.currentThread().isInterrupted())
          throw new InterruptedException("Kafka write interrupted");
        Objects.requireNonNull(row);
        if (row.getRowKind() != RowKind.INSERT || row.getArity() != config.arity)
          throw KafkaConfig.failure();
        Object k = row.getField(config.keyIndex), v = row.getField(config.valueIndex);
        if ((k != null && !(k instanceof byte[])) || (v != null && !(v instanceof byte[])))
          throw KafkaConfig.failure();
        byte[] key = (byte[]) k, value = (byte[]) v;
        if ((key == null && !config.keyNullable)
            || (value == null && !config.valueNullable)
            || (long) (key == null ? 0 : key.length) + (value == null ? 0 : value.length)
                > config.maxBytes) throw KafkaConfig.failure();
        if (pending.size() >= config.maxInFlight) await();
        pending.addLast(
            Objects.requireNonNull(
                client.send(
                    key == null ? null : key.clone(), value == null ? null : value.clone())));
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        throw KafkaConfig.failure();
      }
    }

    public void flush(boolean end) throws Exception {
      active();
      try {
        while (!pending.isEmpty()) await();
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        throw KafkaConfig.failure();
      }
    }

    public List<String> prepareCommit(long id) throws Exception {
      if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
      flush(false);
      return List.of();
    }

    public State snapshotState(long id) {
      active();
      if (id <= 0 || !pending.isEmpty())
        throw new IllegalStateException("Kafka snapshot requires flushed acknowledgements");
      return new State(acknowledged);
    }

    public void notifyCheckpointComplete(long id) {
      active();
      if (id <= 0) throw new IllegalArgumentException("Positive checkpoint required");
    }

    public void notifyCheckpointAborted(long id) {
      notifyCheckpointComplete(id);
    }

    public void close() {
      if (closed) return;
      closed = true;
      pending.clear();
      if (client != null)
        try {
          client.close();
        } catch (Exception ignored) {
          /* May have delivered already; details redacted. */
        }
    }
  }
}
