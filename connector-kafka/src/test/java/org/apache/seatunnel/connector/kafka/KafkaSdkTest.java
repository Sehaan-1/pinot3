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
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;

class KafkaSdkTest {
  @Test
  void delegatesToOfficialSdkMocks() throws Exception {
    var mock = new MockConsumer<byte[], byte[]>("none");
    var tp = new TopicPartition("events", 0);
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.updateEndOffsets(Map.of(tp, 3L));
    mock.schedulePollTask(
        () -> {
          mock.addRecord(new ConsumerRecord<>("events", 0, 0L, new byte[] {1}, new byte[] {2}));
          mock.addRecord(new ConsumerRecord<>("events", 0, 1L, null, null));
          mock.addRecord(new ConsumerRecord<>("events", 0, 2L, null, new byte[] {3}));
        });
    var source =
        new KafkaSource(
            new KafkaConfig(KafkaContracts.options(true), null), c -> KafkaSdk.reader(c, mock));
    try (var reader = source.createReader(0, Optional.empty())) {
      reader.start();
      KafkaContracts.assign(source, reader);
      reader.pollNext(r -> {});
      var state = reader.snapshotState(1);
      KafkaContracts.check(state.offsets().get(0) == 1L);
      reader.notifyCheckpointComplete(1);
      KafkaContracts.check(mock.committed(Set.of(tp)).get(tp).offset() == 1L);
    }
    KafkaContracts.check(mock.closed());
    var producer =
        new MockProducer<byte[], byte[]>(
            true, new ByteArraySerializer(), new ByteArraySerializer());
    var sink =
        new KafkaSink(
            new KafkaConfig(KafkaContracts.options(false), KafkaConfig.TABLE),
            c -> KafkaSdk.writer(c, producer));
    try (var writer = sink.createWriter(0, Optional.empty())) {
      writer.write(
          new org.apache.seatunnel.api.table.SeaTunnelRow("events", 0, 0L, null, new byte[] {4}));
      writer.flush(true);
      KafkaContracts.check(writer.snapshotState(1).acknowledged() == 1);
    }
    KafkaContracts.check(producer.closed() && producer.history().get(0).value()[0] == 4);
  }
}
