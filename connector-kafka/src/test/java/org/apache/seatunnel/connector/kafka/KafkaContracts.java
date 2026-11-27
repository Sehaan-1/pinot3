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
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.*;

/** Deterministic operation-port tests; not Kafka SDK, broker or real delivery evidence. */
final class KafkaContracts {
  static int checks;

  static void check(boolean value) {
    checks++;
    if (!value) throw new AssertionError("check " + checks);
  }

  interface Work {
    void run() throws Exception;
  }

  static Exception reject(Work work) throws Exception {
    checks++;
    try {
      work.run();
    } catch (Exception expected) {
      return expected;
    }
    throw new AssertionError("Expected rejection " + checks);
  }

  static Map<String, Object> options(boolean source) {
    var map =
        new HashMap<String, Object>(
            Map.of(
                "bootstrap-servers",
                List.of("localhost:9092"),
                "topic",
                "events",
                "security-protocol",
                "PLAINTEXT"));
    if (source)
      map.putAll(
          Map.of(
              "group-id",
              "test-group",
              "mode",
              "bounded",
              "partitions",
              List.of(Map.of("partition", 0, "start-offset", 0, "end-offset", 3))));
    return map;
  }

  static final class ReadClient implements KafkaClients.Reader {
    final List<String> events = new ArrayList<>();
    Map<Integer, Long> positions = new HashMap<>();
    Map<Integer, Long> committed = Map.of();
    List<KafkaClients.Message> records =
        new ArrayList<>(
            List.of(
                new KafkaClients.Message("events", 0, 0, new byte[] {1}, new byte[] {10}),
                new KafkaClients.Message("events", 0, 1, null, null),
                new KafkaClients.Message("events", 0, 2, null, new byte[] {12})));
    long first = 0, end = 3;
    String fail = "";

    void event(String value) {
      events.add(value);
      if (fail.equals(value)) throw new IllegalStateException("PRIVATE_CLIENT_SECRET");
    }

    public void assign(List<KafkaSource.Split> splits) {
      event("assign");
      for (var s : splits) positions.put(s.partition(), s.start());
    }

    public Map<Integer, Long> beginnings() {
      event("begin");
      var map = new HashMap<Integer, Long>();
      positions.keySet().forEach(p -> map.put(p, first));
      return map;
    }

    public Map<Integer, Long> ends() {
      event("end");
      var map = new HashMap<Integer, Long>();
      positions.keySet().forEach(p -> map.put(p, end));
      return map;
    }

    public void seek(int p, long offset) {
      event("seek");
      positions.put(p, offset);
    }

    public List<KafkaClients.Message> poll() {
      event("poll");
      var batch =
          records.stream()
              .filter(r -> r.offset() >= positions.getOrDefault(r.partition(), 0L))
              .toList();
      positions.replaceAll((p, o) -> end);
      return batch;
    }

    public long position(int p) {
      event("position");
      return positions.get(p);
    }

    public void pause(Set<Integer> partitions) {
      event("pause");
    }

    public void commit(Map<Integer, Long> offsets) {
      event("commit");
      committed = Map.copyOf(offsets);
    }

    public void close() {
      event("close");
    }
  }

  static final class WriteClient implements KafkaClients.Writer {
    final List<byte[]> keys = new ArrayList<>(), values = new ArrayList<>();
    final List<CompletableFuture<Void>> futures = new ArrayList<>();
    boolean closed, automatic = true;
    String fail = "";

    public Future<?> send(byte[] key, byte[] value) {
      if (fail.equals("send")) throw new IllegalStateException("PRIVATE_CLIENT_SECRET");
      keys.add(key);
      values.add(value);
      var future = new CompletableFuture<Void>();
      if (fail.equals("ack"))
        future.completeExceptionally(new IllegalStateException("PRIVATE_CLIENT_SECRET"));
      else if (automatic) future.complete(null);
      futures.add(future);
      return future;
    }

    public void close() {
      closed = true;
    }
  }

  static void assign(
      KafkaSource source, SourceReader<KafkaSource.Split, KafkaSource.ReaderState> reader)
      throws Exception {
    try (var enumerator =
        source.createEnumerator(
            new SplitEnumerator.Context<KafkaSource.Split>() {
              public int currentParallelism() {
                return 1;
              }

              public void assignSplits(int id, List<KafkaSource.Split> splits) throws Exception {
                reader.addSplits(splits);
              }

              public void signalNoMoreSplits(int id) throws Exception {
                reader.notifyNoMoreSplits();
              }
            },
            Optional.empty())) {
      enumerator.start();
      enumerator.registerReader(0);
      enumerator.handleSplitRequest(0);
      check(enumerator.snapshotState(1).assigned());
    }
  }

  public static void main(String[] args) throws Exception {
    checks = 0;
    var config = new KafkaConfig(options(true), null);
    check(config.properties.get("enable.auto.commit").equals(false));
    check(config.properties.get("isolation.level").equals("read_committed"));
    check(!config.toString().contains("localhost"));
    check(new KafkaSourcePlugin().createSource(options(true)).boundedness() == Boundedness.BOUNDED);
    var live = options(true);
    live.put("mode", "live");
    live.put("partitions", List.of(Map.of("partition", 0, "start-offset", 0)));
    check(new KafkaSourcePlugin().createSource(live).boundedness() == Boundedness.UNBOUNDED);
    var tls = options(true);
    tls.remove("security-protocol");
    check(new KafkaConfig(tls, null).properties.get("security.protocol").equals("SSL"));
    for (String key : List.of("topic", "bootstrap-servers", "group-id", "mode", "partitions")) {
      var bad = options(true);
      bad.remove(key);
      reject(() -> new KafkaConfig(bad, null));
    }
    for (String key : List.of("poll-ms", "max-poll-records", "timeout-ms", "max-record-bytes"))
      for (Object bad : List.of(0, -1, "1", 1.5)) {
        var map = options(true);
        map.put(key, bad);
        reject(() -> new KafkaConfig(map, null));
      }
    for (Object parts :
        List.of(
            List.of(),
            List.of(Map.of("partition", 0, "start-offset", 3, "end-offset", 2)),
            List.of(Map.of("partition", 0, "start-offset", 0)),
            Collections.nCopies(2, Map.of("partition", 0, "start-offset", 0, "end-offset", 1)))) {
      var bad = options(true);
      bad.put("partitions", parts);
      reject(() -> new KafkaConfig(bad, null));
    }
    for (String security : List.of("SASL_PLAINTEXT", "unknown")) {
      var bad = options(true);
      bad.put("security-protocol", security);
      reject(() -> new KafkaConfig(bad, null));
    }
    var sasl = options(true);
    sasl.put("security-protocol", "SASL_SSL");
    reject(() -> new KafkaConfig(sasl, null));
    sasl.put("sasl-mechanism", "PLAIN");
    sasl.put("sasl-jaas-config", "PRIVATE_CLIENT_SECRET");
    var secret = new KafkaConfig(sasl, null);
    check(!secret.toString().contains("PRIVATE_CLIENT_SECRET"));
    var ssl = options(true);
    ssl.put("ssl-truststore-location", "/private");
    reject(() -> new KafkaConfig(ssl, null));
    var badField = options(false);
    badField.put("key-field", "offset");
    reject(() -> new KafkaConfig(badField, KafkaConfig.TABLE));
    var client = new ReadClient();
    var source = new KafkaSource(config, c -> client);
    var reader = source.createReader(0, Optional.empty());
    check(client.events.isEmpty());
    reject(() -> reader.pollNext(r -> {}));
    reader.start();
    reject(reader::start);
    assign(source, reader);
    reject(() -> reader.addSplits(config.splits));
    var rows = new ArrayList<SeaTunnelRow>();
    reader.pollNext(rows::add);
    var snapshot = reader.snapshotState(1);
    check(snapshot.offsets().get(0) == 1L && client.positions.get(0) == 3L);
    reject(() -> snapshot.offsets().clear());
    reject(() -> reader.snapshotState(2));
    reader.pollNext(rows::add);
    check(rows.get(1).getField(4) == null);
    reader.notifyCheckpointComplete(1);
    check(client.committed.get(0) == 1L);
    reader.notifyCheckpointComplete(1);
    check(Collections.frequency(client.events, "commit") == 1);
    reader.snapshotState(2);
    reader.notifyCheckpointAborted(2);
    check(client.committed.get(0) == 1L);
    reader.close();
    reader.close();
    check(Collections.frequency(client.events, "close") == 1);
    var replacement = new ReadClient();
    var restoredSource = new KafkaSource(config, c -> replacement);
    try (var r = restoredSource.createReader(0, Optional.of(snapshot))) {
      r.start();
      r.notifyCheckpointComplete(1);
      check(replacement.positions.get(0) == 1L && replacement.committed.get(0) == 1L);
      var rest = new ArrayList<SeaTunnelRow>();
      while (r.pollNext(rest::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
      check(rest.size() == 2 && (Long) rest.get(0).getField(2) == 1L);
    }
    var holes = new ReadClient();
    holes.records = List.of();
    var holeSource = new KafkaSource(config, c -> holes);
    try (var r = holeSource.createReader(0, Optional.empty())) {
      r.start();
      assign(holeSource, r);
      check(
          r.pollNext(
                  x -> {
                    throw new AssertionError();
                  })
              == SourceReader.ReadStatus.END_OF_INPUT);
      check(r.snapshotState(1).offsets().get(0) == 3L);
    }
    var expired = new ReadClient();
    expired.first = 1;
    var expiredSource = new KafkaSource(config, c -> expired);
    var expiredReader = expiredSource.createReader(0, Optional.empty());
    expiredReader.start();
    reject(() -> assign(expiredSource, expiredReader));
    check(expired.events.contains("close"));
    for (String failure : List.of("assign", "begin", "end", "seek", "poll", "position", "commit")) {
      var c = new ReadClient();
      var s = new KafkaSource(config, x -> c);
      var r = s.createReader(0, Optional.empty());
      r.start();
      if (List.of("assign", "begin", "end", "seek").contains(failure)) {
        c.fail = failure;
        reject(() -> assign(s, r));
      } else {
        assign(s, r);
        c.fail = failure;
        if (failure.equals("commit")) {
          r.pollNext(x -> {});
          r.snapshotState(1);
          reject(() -> r.notifyCheckpointComplete(1));
        } else if (failure.equals("position")) {
          r.pollNext(x -> {});
          r.pollNext(x -> {});
          reject(() -> r.pollNext(x -> {}));
        } else reject(() -> r.pollNext(x -> {}));
      }
      check(c.events.contains("close"));
    }
    var output = new ReadClient();
    var outputSource = new KafkaSource(config, c -> output);
    var outputReader = outputSource.createReader(0, Optional.empty());
    outputReader.start();
    assign(outputSource, outputReader);
    check(
        !reject(
                () ->
                    outputReader.pollNext(
                        r -> {
                          throw new IllegalStateException("downstream");
                        }))
            .toString()
            .contains("PRIVATE_CLIENT_SECRET"));
    check(output.events.contains("close"));
    var badState =
        new KafkaSource.ReaderState("other", snapshot.splits(), snapshot.offsets(), true, 1);
    reject(() -> source.createReader(0, Optional.of(badState)));
    reject(() -> source.createReader(1, Optional.empty()));
    var idle = new ReadClient();
    idle.end = 0;
    idle.records = List.of();
    var liveSource = new KafkaSource(new KafkaConfig(live, null), c -> idle);
    try (var r = liveSource.createReader(0, Optional.empty())) {
      r.start();
      assign(liveSource, r);
      check(r.pollNext(x -> {}) == SourceReader.ReadStatus.NOTHING_AVAILABLE);
    }
    var sinkConfig = new KafkaConfig(options(false), KafkaConfig.TABLE);
    check(
        sinkConfig.properties.get("enable.idempotence").equals(true)
            && sinkConfig.properties.get("acks").equals("all"));
    var sends = new WriteClient();
    var sink = new KafkaSink(sinkConfig, c -> sends);
    check(sink.createCommitter().isEmpty());
    var writer = sink.createWriter(0, Optional.empty());
    byte[] value = {7};
    var row = new SeaTunnelRow("events", 0, 0L, null, value);
    writer.write(row);
    value[0] = 99;
    check(sends.values.get(0)[0] == 7);
    reject(() -> writer.snapshotState(1));
    writer.flush(false);
    check(writer.snapshotState(1).acknowledged() == 1);
    check(writer.prepareCommit(1).isEmpty());
    writer.write(new SeaTunnelRow("events", 0, 1L, null, null));
    writer.flush(true);
    check(writer.snapshotState(2).acknowledged() == 2);
    writer.close();
    writer.close();
    check(sends.closed);
    reject(() -> writer.write(row));
    var restoreSends = new WriteClient();
    try (var w =
        new KafkaSink(sinkConfig, c -> restoreSends)
            .createWriter(0, Optional.of(new KafkaSink.State(2)))) {
      check(w.snapshotState(3).acknowledged() == 2);
    }
    for (RowKind kind : RowKind.values())
      if (kind != RowKind.INSERT) {
        var c = new WriteClient();
        var w = new KafkaSink(sinkConfig, x -> c).createWriter(0, Optional.empty());
        reject(() -> w.write(row.withKind(kind)));
        check(c.closed && c.values.isEmpty());
      }
    for (String failure : List.of("send", "ack")) {
      var c = new WriteClient();
      c.fail = failure;
      var w = new KafkaSink(sinkConfig, x -> c).createWriter(0, Optional.empty());
      var error =
          reject(
              () -> {
                w.write(row);
                w.flush(true);
              });
      check(
          !error.toString().contains("PRIVATE_CLIENT_SECRET")
              && error.getCause() == null
              && c.closed);
    }
    var window = options(false);
    window.put("max-in-flight-records", 1);
    var windowClient = new WriteClient();
    windowClient.automatic = false;
    var windowSink = new KafkaSink(new KafkaConfig(window, KafkaConfig.TABLE), c -> windowClient);
    var w = windowSink.createWriter(0, Optional.empty());
    w.write(row);
    windowClient.futures.get(0).complete(null);
    w.write(row);
    check(windowClient.values.size() == 2);
    windowClient.futures.get(1).complete(null);
    w.flush(true);
    check(w.snapshotState(1).acknowledged() == 2);
    w.close();
    var interrupted = new ReadClient();
    var interruptSource = new KafkaSource(config, c -> interrupted);
    var interruptedReader = interruptSource.createReader(0, Optional.empty());
    interruptedReader.start();
    assign(interruptSource, interruptedReader);
    Thread.currentThread().interrupt();
    try {
      check(reject(() -> interruptedReader.pollNext(x -> {})) instanceof InterruptedException);
    } finally {
      Thread.interrupted();
    }
    check(interrupted.events.contains("close"));
    System.out.println(
        "PASS Kafka adapter/config contracts: "
            + checks
            + " assertions; TEST operation ports, NOT SDK/broker proof");
  }
}
