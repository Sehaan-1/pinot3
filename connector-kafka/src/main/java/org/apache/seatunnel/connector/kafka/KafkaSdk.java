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
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

/** Official Kafka SDK owns network, codecs, retries, authentication and internal buffering. */
final class KafkaSdk {
  private KafkaSdk() {}

  static KafkaClients.Reader reader(KafkaConfig config) {
    return reader(config, new KafkaConsumer<byte[], byte[]>(config.properties));
  }

  static KafkaClients.Writer writer(KafkaConfig config) {
    return writer(config, new KafkaProducer<byte[], byte[]>(config.properties));
  }

  static KafkaClients.Reader reader(KafkaConfig config, Consumer<byte[], byte[]> consumer) {
    return new KafkaClients.Reader() {
      final Duration timeout = Duration.ofMillis(config.timeout);
      Set<TopicPartition> assigned = Set.of();

      TopicPartition tp(int partition) {
        return new TopicPartition(config.topic, partition);
      }

      Map<Integer, Long> plain(Map<TopicPartition, Long> values) {
        return values.entrySet().stream()
            .collect(
                Collectors.toUnmodifiableMap(e -> e.getKey().partition(), Map.Entry::getValue));
      }

      public void assign(List<KafkaSource.Split> splits) {
        assigned =
            splits.stream().map(s -> tp(s.partition())).collect(Collectors.toUnmodifiableSet());
        consumer.assign(assigned);
      }

      public Map<Integer, Long> beginnings() {
        return plain(consumer.beginningOffsets(assigned, timeout));
      }

      public Map<Integer, Long> ends() {
        return plain(consumer.endOffsets(assigned, timeout));
      }

      public void seek(int partition, long offset) {
        consumer.seek(tp(partition), offset);
      }

      public List<KafkaClients.Message> poll() {
        var records = consumer.poll(Duration.ofMillis(config.poll));
        var messages = new ArrayList<KafkaClients.Message>(records.count());
        for (var r : records)
          messages.add(
              new KafkaClients.Message(r.topic(), r.partition(), r.offset(), r.key(), r.value()));
        return List.copyOf(messages);
      }

      public long position(int partition) {
        return consumer.position(tp(partition), timeout);
      }

      public void pause(Set<Integer> partitions) {
        consumer.pause(partitions.stream().map(this::tp).toList());
      }

      public void commit(Map<Integer, Long> offsets) {
        var map = new HashMap<TopicPartition, OffsetAndMetadata>();
        offsets.forEach((p, o) -> map.put(tp(p), new OffsetAndMetadata(o)));
        consumer.commitSync(map, timeout);
      }

      public void close() {
        consumer.close(Duration.ZERO);
      }
    };
  }

  static KafkaClients.Writer writer(KafkaConfig config, Producer<byte[], byte[]> producer) {
    return new KafkaClients.Writer() {
      public Future<?> send(byte[] key, byte[] value) {
        return producer.send(new ProducerRecord<>(config.topic, key, value));
      }

      public void close() {
        producer.close(Duration.ZERO);
      }
    };
  }
}
