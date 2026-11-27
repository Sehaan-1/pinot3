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

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.engine.checkpoint.*;
import org.apache.seatunnel.engine.cluster.*;
import org.apache.seatunnel.engine.streaming.StreamingRunner;

/** Real broker + real client + real checkpoint store. Called only by explicitly enabled gates. */
final class KafkaBrokerScenarios {
  static Map<String, Object> options(
      String brokers, String topic, boolean source, int partitions, int end) {
    var options = KafkaContracts.options(source);
    options.put("bootstrap-servers", List.of(brokers.split(",")));
    options.put("topic", topic);
    options.put("timeout-ms", 10000);
    if (source) {
      options.put("group-id", "gate-" + UUID.randomUUID());
      var ranges = new ArrayList<Map<String, Object>>();
      for (int p = 0; p < partitions; p++)
        ranges.add(Map.of("partition", p, "start-offset", 0, "end-offset", end));
      options.put("partitions", ranges);
    }
    return options;
  }

  static Properties clientProperties(String brokers) {
    var p = new Properties();
    p.setProperty("bootstrap.servers", brokers);
    p.setProperty("security.protocol", "PLAINTEXT");
    p.setProperty("default.api.timeout.ms", "10000");
    p.setProperty("request.timeout.ms", "5000");
    return p;
  }

  static void seed(String brokers, String topic, int partitions, int count) throws Exception {
    var properties = clientProperties(brokers);
    properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    properties.put("acks", "all");
    properties.put("delivery.timeout.ms", 10000);
    try (var producer = new KafkaProducer<byte[], byte[]>(properties)) {
      for (int p = 0; p < partitions; p++)
        for (int i = 0; i < count; i++)
          producer
              .send(
                  new ProducerRecord<>(
                      topic,
                      p,
                      new byte[] {(byte) p, (byte) i},
                      i == 1 ? null : new byte[] {(byte) (10 + i)}))
              .get(15, TimeUnit.SECONDS);
    }
  }

  static List<SeaTunnelRow> drain(SourceReader<KafkaSource.Split, KafkaSource.ReaderState> reader)
      throws Exception {
    var rows = new ArrayList<SeaTunnelRow>();
    long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
    while (reader.pollNext(rows::add) != SourceReader.ReadStatus.END_OF_INPUT)
      if (System.nanoTime() > end)
        throw new AssertionError("Broker bounded read did not terminate");
    return rows;
  }

  static void run(String brokers) throws Exception {
    String input = "pinot-in-" + UUID.randomUUID(),
        output = "pinot-out-" + UUID.randomUUID(),
        live = "pinot-live-" + UUID.randomUUID(),
        mirror = "pinot-mirror-" + UUID.randomUUID();
    var adminProperties = clientProperties(brokers);
    try (var admin = Admin.create(adminProperties)) {
      admin
          .createTopics(
              List.of(
                  new NewTopic(input, 2, (short) 1),
                  new NewTopic(output, 1, (short) 1),
                  new NewTopic(live, 1, (short) 1),
                  new NewTopic(mirror, 1, (short) 1)))
          .all()
          .get(30, TimeUnit.SECONDS);
      try {
        seed(brokers, input, 2, 3);
        var read = options(brokers, input, true, 2, 2);
        var source = new KafkaSourcePlugin().createSource(read);
        KafkaSource.ReaderState saved;
        var prefix = new ArrayList<SeaTunnelRow>();
        try (var reader = source.createReader(0, Optional.empty())) {
          reader.start();
          KafkaContracts.assign(source, reader);
          long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
          while (prefix.isEmpty()) {
            reader.pollNext(prefix::add);
            if (System.nanoTime() > deadline) throw new AssertionError("No broker row");
          }
          saved = reader.snapshotState(1);
        }
        var all = new ArrayList<>(prefix);
        try (var reader =
            new KafkaSourcePlugin().createSource(read).createReader(0, Optional.of(saved))) {
          reader.start();
          reader.notifyCheckpointComplete(1);
          all.addAll(drain(reader));
        }
        KafkaContracts.check(all.size() == 4);
        KafkaContracts.check(
            all.stream().map(r -> r.getField(1) + ":" + r.getField(2)).distinct().count() == 4);
        KafkaContracts.check(all.stream().filter(r -> r.getField(4) == null).count() == 2);
        var committed =
            admin
                .listConsumerGroupOffsets((String) read.get("group-id"))
                .partitionsToOffsetAndMetadata()
                .get(20, TimeUnit.SECONDS);
        saved
            .offsets()
            .forEach(
                (p, o) ->
                    KafkaContracts.check(
                        committed.get(new TopicPartition(input, p)).offset() == o));
        var sink =
            new KafkaSinkPlugin()
                .createSink(options(brokers, output, false, 1, 0), KafkaConfig.TABLE, null);
        try (var writer = sink.createWriter(0, Optional.empty())) {
          for (var row : all) writer.write(row);
          writer.flush(true);
          KafkaContracts.check(writer.snapshotState(1).acknowledged() == 4);
        }
        var outSource = new KafkaSourcePlugin().createSource(options(brokers, output, true, 1, 4));
        try (var reader = outSource.createReader(0, Optional.empty())) {
          reader.start();
          KafkaContracts.assign(outSource, reader);
          var result = drain(reader);
          KafkaContracts.check(
              result.size() == 4
                  && result.stream().filter(r -> r.getField(4) == null).count() == 2);
        }
        // Existing partition but configured end beyond readable LSO must fail, never reset/clamp
        // silently.
        var invalid = options(brokers, input, true, 1, 1000);
        var invalidSource = new KafkaSourcePlugin().createSource(invalid);
        try (var reader = invalidSource.createReader(0, Optional.empty())) {
          reader.start();
          KafkaContracts.reject(() -> KafkaContracts.assign(invalidSource, reader));
          KafkaContracts.reject(() -> reader.pollNext(r -> {}));
        }
        // Real live source -> acknowledged Kafka sink -> replicated in-memory engine checkpoint ->
        // restart.
        seed(brokers, live, 1, 1);
        var liveOptions = options(brokers, live, true, 1, 0);
        liveOptions.put("mode", "live");
        liveOptions.put("partitions", List.of(Map.of("partition", 0, "start-offset", 0)));
        var nodeSettings =
            new EngineSettings(
                "kafka-gate-" + UUID.randomUUID(),
                "127.0.0.1",
                0,
                List.of(),
                2,
                16,
                2,
                Duration.ofSeconds(5));
        try (var node = EngineNode.start(nodeSettings)) {
          var store = new CheckpointStore(node.member());
          UUID key = UUID.randomUUID();
          runUntilCheckpoint(
              key, liveOptions, options(brokers, mirror, false, 1, 0), store, 1, false);
          // Add offset1 after the first run is stopped; same source group/config/binding on
          // restore.
          seed(brokers, live, 1, 1);
          runUntilCheckpoint(
              key, liveOptions, options(brokers, mirror, false, 1, 0), store, 2, true);
          var state =
              (StreamingRunner.Snapshot) store.get(key).participants().get("pipeline").state();
          KafkaContracts.check(
              state.rows() == 2
                  && ((KafkaSource.ReaderState) state.reader()).offsets().get(0) == 2L);
          var mirrorSource =
              new KafkaSourcePlugin().createSource(options(brokers, mirror, true, 1, 2));
          try (var reader = mirrorSource.createReader(0, Optional.empty())) {
            reader.start();
            KafkaContracts.assign(mirrorSource, reader);
            KafkaContracts.check(drain(reader).size() == 2);
          }
          if (store.get(key).status() == CheckpointRecord.Status.COMPLETED)
            store.delete(key); // Cancellation may retain a pending decision.
        }
      } finally {
        admin.deleteTopics(List.of(input, output, live, mirror)).all().get(30, TimeUnit.SECONDS);
      }
    }
  }

  static void runUntilCheckpoint(
      UUID key,
      Map<String, Object> input,
      Map<String, Object> output,
      CheckpointStore store,
      long count,
      boolean restore)
      throws Exception {
    var error = new AtomicReference<Throwable>();
    var thread =
        new Thread(
            () -> {
              try {
                var source = new KafkaSourcePlugin().createSource(input);
                var sink = new KafkaSinkPlugin().createSink(output, KafkaConfig.TABLE, null);
                new StreamingRunner()
                    .run(
                        key,
                        new StreamingRunner.Pipeline<>(
                            "kafka-live-v1",
                            source,
                            sink,
                            KafkaSource.ReaderState.class,
                            KafkaSource.EnumeratorState.class,
                            KafkaSink.State.class,
                            String.class),
                        store,
                        new StreamingRunner.Settings(100, 5, 1, restore));
                error.set(new AssertionError("Live run ended unexpectedly"));
              } catch (InterruptedException expected) {
              } catch (Throwable t) {
                error.set(t);
              }
            });
    thread.start();
    try {
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
      while (true) {
        if (error.get() != null) throw new AssertionError("Live runner failed", error.get());
        var record = store.get(key);
        if (record != null
            && record.status() == CheckpointRecord.Status.COMPLETED
            && ((StreamingRunner.Snapshot) record.participants().get("pipeline").state()).rows()
                >= count) break;
        if (System.nanoTime() > deadline) throw new AssertionError("Live checkpoint timed out");
        Thread.sleep(20);
      }
    } finally {
      thread.interrupt();
      thread.join(15000);
    }
    KafkaContracts.check(!thread.isAlive() && error.get() == null);
  }
}
