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
import java.util.concurrent.Future;

/** Narrow adapters of official client operations, not alternative protocols or retry engines. */
final class KafkaClients {
  private KafkaClients() {}

  record Message(String topic, int partition, long offset, byte[] key, byte[] value) {
    Message {
      key = key == null ? null : key.clone();
      value = value == null ? null : value.clone();
    }
  }

  interface Reader extends AutoCloseable {
    void assign(List<KafkaSource.Split> splits);

    Map<Integer, Long> beginnings();

    Map<Integer, Long> ends();

    void seek(int partition, long offset);

    List<Message> poll();

    long position(int partition);

    void pause(Set<Integer> partitions);

    void commit(Map<Integer, Long> offsets);

    void close();
  }

  interface Writer extends AutoCloseable {
    Future<?> send(byte[] key, byte[] value);

    void close();
  }

  @FunctionalInterface
  interface ReaderFactory {
    Reader open(KafkaConfig config);
  }

  @FunctionalInterface
  interface WriterFactory {
    Writer open(KafkaConfig config);
  }
}
