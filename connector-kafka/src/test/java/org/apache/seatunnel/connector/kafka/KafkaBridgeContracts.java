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

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.seatunnel.engine.checkpoint.*;
import org.apache.seatunnel.engine.streaming.StreamingRunner;

/** Real adapter + existing runtime bridge, test-only ports/repository; not SDK/broker evidence. */
final class KafkaBridgeContracts {
  static final class Repository implements CheckpointRepository {
    volatile CheckpointRecord record;

    @SuppressWarnings("unchecked")
    static <T extends Serializable> T copy(T value) {
      try {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) {
          out.writeObject(value);
        }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
          return (T) in.readObject();
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    public CheckpointRecord get(UUID key) {
      return record == null ? null : copy(record);
    }

    public boolean publish(UUID key, CheckpointRecord expected, CheckpointRecord next) {
      record = copy(next);
      return true;
    }

    public void complete(UUID key, CheckpointRecord decision) {
      record = copy(decision.completed());
    }
  }

  public static void main(String[] args) throws Exception {
    UUID key = UUID.randomUUID();
    var repo = new Repository();
    var input = KafkaContracts.options(true);
    input.put("mode", "live");
    input.put("partitions", List.of(Map.of("partition", 0, "start-offset", 0)));
    var first = new KafkaContracts.ReadClient();
    var writes = new KafkaContracts.WriteClient();
    run(key, input, first, writes, repo, 3, false);
    KafkaContracts.check(writes.values.size() == 3 && first.committed.get(0) == 3L);
    var second = new KafkaContracts.ReadClient();
    second.end = 4;
    second.records = new ArrayList<>(second.records);
    second.records.add(new KafkaClients.Message("events", 0, 3, null, new byte[] {13}));
    var resumed = new KafkaContracts.WriteClient();
    run(key, input, second, resumed, repo, 4, true);
    KafkaContracts.check(
        resumed.values.size() == 1
            && resumed.values.get(0)[0] == 13
            && second.committed.get(0) == 4L);
    var snapshot = (StreamingRunner.Snapshot) repo.record.participants().get("pipeline").state();
    KafkaContracts.check(
        snapshot.rows() == 4 && ((KafkaSink.State) snapshot.writer()).acknowledged() == 4);
    System.out.println(
        "PASS Kafka source/sink -> StreamingRunner checkpoint/live cancellation/serialized"
            + " restore/exact group-offset scenario (TEST client ports/repository, NOT broker"
            + " proof)");
  }

  static void run(
      UUID key,
      Map<String, Object> input,
      KafkaContracts.ReadClient reader,
      KafkaContracts.WriteClient writer,
      Repository repo,
      long count,
      boolean restore)
      throws Exception {
    var error = new AtomicReference<Throwable>();
    var source = new KafkaSource(new KafkaConfig(input, null), c -> reader);
    var sink =
        new KafkaSink(
            new KafkaConfig(KafkaContracts.options(false), KafkaConfig.TABLE), c -> writer);
    var thread =
        new Thread(
            () -> {
              try {
                new StreamingRunner()
                    .run(
                        key,
                        new StreamingRunner.Pipeline<>(
                            "kafka-port-v1",
                            source,
                            sink,
                            KafkaSource.ReaderState.class,
                            KafkaSource.EnumeratorState.class,
                            KafkaSink.State.class,
                            String.class),
                        repo,
                        new StreamingRunner.Settings(1, 2, 1, restore));
                error.set(new AssertionError("Live source ended"));
              } catch (InterruptedException expected) {
              } catch (Throwable t) {
                error.set(t);
              }
            });
    thread.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (true) {
        if (error.get() != null) throw new AssertionError(error.get());
        var record = repo.record;
        if (record != null
            && record.status() == CheckpointRecord.Status.COMPLETED
            && ((StreamingRunner.Snapshot) record.participants().get("pipeline").state()).rows()
                >= count) break;
        if (System.nanoTime() > deadline) throw new AssertionError("No checkpoint");
        Thread.sleep(2);
      }
    } finally {
      thread.interrupt();
      thread.join(5000);
    }
    KafkaContracts.check(
        !thread.isAlive()
            && error.get() == null
            && writer.closed
            && reader.events.contains("close"));
  }
}
