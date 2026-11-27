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
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.*;
import org.apache.seatunnel.common.exception.ConfigException;

/** Strict resource-free configuration. Never generate a credential-bearing toString. */
final class KafkaConfig {
  final String topic, group, mode;
  final int timeout, poll, maxPoll, maxBytes, maxInFlight, keyIndex, valueIndex, arity;
  final List<KafkaSource.Split> splits;
  final Map<String, Object> properties;
  final boolean keyNullable, valueNullable;
  static final CatalogTable TABLE =
      new CatalogTable(
          "kafka",
          List.of(
              new Column("topic", DataType.STRING, false),
              new Column("partition", DataType.INT, false),
              new Column("offset", DataType.LONG, false),
              new Column("key", DataType.BYTES, true),
              new Column("value", DataType.BYTES, true)));

  KafkaConfig(Map<String, Object> options, CatalogTable input) {
    var allowed =
        new HashSet<>(
            Set.of(
                "bootstrap-servers",
                "topic",
                "security-protocol",
                "sasl-mechanism",
                "sasl-jaas-config",
                "ssl-truststore-location",
                "ssl-truststore-password",
                "ssl-keystore-location",
                "ssl-keystore-password",
                "ssl-key-password",
                "timeout-ms",
                "max-record-bytes"));
    allowed.addAll(
        input == null
            ? Set.of("group-id", "mode", "partitions", "poll-ms", "max-poll-records")
            : Set.of("key-field", "value-field", "max-in-flight-records"));
    if (!allowed.containsAll(options.keySet())) throw bad("Unknown Kafka option");
    topic = text(options, "topic");
    if (!topic.matches("[a-zA-Z0-9._-]{1,249}") || Set.of(".", "..").contains(topic))
      throw bad("Invalid Kafka topic");
    if (!(options.get("bootstrap-servers") instanceof List<?> servers)
        || servers.isEmpty()
        || servers.size() > 16) throw bad("bootstrap-servers requires 1..16 strings");
    for (Object server : servers)
      if (!(server instanceof String s)
          || s.isBlank()
          || s.length() > 256
          || s.contains("\n")
          || s.contains("\r")) throw bad("Invalid bootstrap server");
    timeout = integer(options, "timeout-ms", 10000, 1000, 60000);
    maxBytes = integer(options, "max-record-bytes", 1048576, 1, 16777216);
    poll = integer(options, "poll-ms", 100, 1, 1000);
    maxPoll = integer(options, "max-poll-records", 100, 1, 1000);
    maxInFlight = integer(options, "max-in-flight-records", 100, 1, 1000);
    var props = new HashMap<String, Object>();
    props.put("bootstrap.servers", List.copyOf(servers));
    String security =
        options.containsKey("security-protocol") ? text(options, "security-protocol") : "SSL";
    if (!Set.of("SSL", "PLAINTEXT", "SASL_SSL").contains(security))
      throw bad("Unsupported security-protocol");
    props.put("security.protocol", security);
    for (var pair :
        List.of(
            List.of("ssl-truststore-location", "ssl-truststore-password"),
            List.of("ssl-keystore-location", "ssl-keystore-password", "ssl-key-password"))) {
      long count = pair.stream().filter(options::containsKey).count();
      if (count != 0 && (count != pair.size() || security.equals("PLAINTEXT")))
        throw bad("Incomplete/inapplicable TLS options");
      if (count != 0) for (String key : pair) props.put(key.replace('-', '.'), text(options, key));
    }
    boolean sasl = options.containsKey("sasl-mechanism") || options.containsKey("sasl-jaas-config");
    if (sasl != security.equals("SASL_SSL")) throw bad("SASL options require SASL_SSL");
    if (sasl) {
      String mechanism = text(options, "sasl-mechanism");
      if (!Set.of("PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512").contains(mechanism))
        throw bad("Unsupported SASL mechanism");
      props.put("sasl.mechanism", mechanism);
      props.put("sasl.jaas.config", text(options, "sasl-jaas-config"));
    }
    props.put("request.timeout.ms", Math.max(1000, timeout / 2));
    props.put("client.id", "seatunnel-kafka");
    if (input == null) {
      group = text(options, "group-id");
      if (group.isBlank() || group.length() > 249) throw bad("Invalid group-id");
      mode = text(options, "mode");
      if (!Set.of("bounded", "live").contains(mode)) throw bad("mode must be bounded or live");
      if (!(options.get("partitions") instanceof List<?> partitions)
          || partitions.isEmpty()
          || partitions.size() > 128) throw bad("partitions requires 1..128 entries");
      var result = new ArrayList<KafkaSource.Split>();
      var seen = new HashSet<Integer>();
      for (Object raw : partitions) {
        if (!(raw instanceof Map<?, ?> range)
            || !range
                .keySet()
                .equals(
                    mode.equals("bounded")
                        ? Set.of("partition", "start-offset", "end-offset")
                        : Set.of("partition", "start-offset")))
          throw bad("Invalid Kafka range options");
        long partition = number(range.get("partition"));
        long start = number(range.get("start-offset"));
        Long end = mode.equals("bounded") ? number(range.get("end-offset")) : null;
        if (partition < 0 || partition > Integer.MAX_VALUE || !seen.add((int) partition))
          throw bad("Invalid/duplicate partition");
        result.add(new KafkaSource.Split(topic, (int) partition, start, end));
      }
      splits = List.copyOf(result);
      keyIndex = 3;
      valueIndex = 4;
      arity = 5;
      keyNullable = true;
      valueNullable = true;
      props.put("group.id", group);
      props.put("enable.auto.commit", false);
      props.put("auto.offset.reset", "none");
      props.put("isolation.level", "read_committed");
      props.put("max.poll.records", maxPoll);
      props.put("default.api.timeout.ms", timeout);
      props.put("max.partition.fetch.bytes", maxBytes);
      props.put("fetch.max.bytes", Math.max(maxBytes, 1048576));
      props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
      props.put(
          "value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    } else {
      group = null;
      mode = null;
      splits = List.of();
      arity = input.columns().size();
      keyIndex =
          field(input, options.containsKey("key-field") ? text(options, "key-field") : "key");
      valueIndex =
          field(input, options.containsKey("value-field") ? text(options, "value-field") : "value");
      if (keyIndex == valueIndex) throw bad("Key/value fields must differ");
      keyNullable = input.columns().get(keyIndex).nullable();
      valueNullable = input.columns().get(valueIndex).nullable();
      props.put("acks", "all");
      props.put("enable.idempotence", true);
      props.put("retries", Integer.MAX_VALUE);
      props.put("delivery.timeout.ms", timeout);
      props.put("max.block.ms", timeout);
      props.put("linger.ms", 0);
      props.put("buffer.memory", 33554432L);
      props.put("max.request.size", maxBytes + 4096);
      props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
      props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
    }
    properties = Map.copyOf(props);
  }

  private static int field(CatalogTable input, String name) {
    for (int i = 0; i < input.columns().size(); i++)
      if (input.columns().get(i).name().equals(name)) {
        if (input.columns().get(i).type() != DataType.BYTES)
          throw bad("Kafka key/value fields require BYTES");
        return i;
      }
    throw bad("Unknown Kafka field");
  }

  static String text(Map<String, Object> options, String key) {
    if (!(options.get(key) instanceof String value))
      throw bad("Missing/invalid Kafka option: " + key);
    return value;
  }

  static long number(Object value) {
    if (value instanceof Integer i) return i.longValue();
    if (value instanceof Long l) return l;
    throw bad("Kafka offsets/options require integral values");
  }

  static int integer(Map<String, Object> options, String key, int fallback, int min, int max) {
    if (!options.containsKey(key)) return fallback;
    long n = number(options.get(key));
    if (n < min || n > max) throw bad("Kafka option out of range: " + key);
    return (int) n;
  }

  static ConfigException bad(String message) {
    return new ConfigException(message);
  }

  static IllegalStateException failure() {
    return new IllegalStateException("Kafka operation failed (client details redacted)");
  }
}
