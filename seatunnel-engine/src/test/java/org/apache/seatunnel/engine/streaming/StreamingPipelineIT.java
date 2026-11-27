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

import static org.assertj.core.api.Assertions.*;

import java.io.StringWriter;
import java.time.Duration;
import java.util.*;
import org.apache.seatunnel.connector.console.ConsoleSink;
import org.apache.seatunnel.connector.fake.FakeSource;
import org.apache.seatunnel.engine.checkpoint.*;
import org.apache.seatunnel.engine.cluster.*;
import org.junit.jupiter.api.Test;

/**
 * Real Hazelcast-backed checkpoint + real existing source/sink. Not broker/DB/exactly-once
 * evidence.
 */
class StreamingPipelineIT {
  @Test
  void runsAndRecognizesCompletedInputThroughRealStore() throws Exception {
    var settings =
        new EngineSettings(
            "stream-it-" + UUID.randomUUID(),
            "127.0.0.1",
            0,
            List.of(),
            2,
            16,
            2,
            Duration.ofSeconds(5));
    try (var node = EngineNode.start(settings)) {
      var store = new CheckpointStore(node.member());
      UUID key = UUID.randomUUID();
      var output = new StringWriter();
      var pipeline =
          new StreamingRunner.Pipeline<>(
              "fake-console-v1",
              new FakeSource(3, 10, 1),
              new ConsoleSink(output),
              FakeSource.ReaderState.class,
              FakeSource.EnumeratorState.class,
              ConsoleSink.State.class,
              String.class);
      assertThat(
              new StreamingRunner()
                  .run(key, pipeline, store, new StreamingRunner.Settings(1000, 1, 1)))
          .isEqualTo(3);
      assertThat(output.toString())
          .isEqualTo("[10,\"row-10\"]\n[11,\"row-11\"]\n[12,\"row-12\"]\n");
      assertThat(store.get(key).status()).isEqualTo(CheckpointRecord.Status.COMPLETED);
      var restored = new StringWriter();
      var second =
          new StreamingRunner.Pipeline<>(
              "fake-console-v1",
              new FakeSource(3, 10, 1),
              new ConsoleSink(restored),
              FakeSource.ReaderState.class,
              FakeSource.EnumeratorState.class,
              ConsoleSink.State.class,
              String.class);
      assertThat(
              new StreamingRunner()
                  .run(key, second, store, new StreamingRunner.Settings(1000, 1, 1, true)))
          .isEqualTo(3);
      assertThat(restored.toString()).isEmpty();
      store.delete(key);
    }
  }
}
