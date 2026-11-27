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

import java.io.*;
import java.util.*;
import org.apache.seatunnel.connector.console.ConsoleSink;
import org.apache.seatunnel.connector.fake.FakeSource;

final class StreamingAdapters {
  public static void main(String[] args) throws Exception {
    var out = new StringWriter();
    var source = new FakeSource(3, 10, 1);
    var pipeline =
        new StreamingRunner.Pipeline<>(
            "fake-console-v1",
            source,
            new ConsoleSink(out),
            FakeSource.ReaderState.class,
            FakeSource.EnumeratorState.class,
            ConsoleSink.State.class,
            String.class);
    var repository = new StreamingContracts.Repository(new ArrayList<>());
    UUID key = UUID.randomUUID();
    long rows =
        new StreamingRunner()
            .run(key, pipeline, repository, new StreamingRunner.Settings(1000, 1, 1));
    if (rows != 3 || !out.toString().equals("[10,\"row-10\"]\n[11,\"row-11\"]\n[12,\"row-12\"]\n"))
      throw new AssertionError(out);
    var second = new StringWriter();
    var restarted =
        new StreamingRunner.Pipeline<>(
            "fake-console-v1",
            new FakeSource(3, 10, 1),
            new ConsoleSink(second),
            FakeSource.ReaderState.class,
            FakeSource.EnumeratorState.class,
            ConsoleSink.State.class,
            String.class);
    if (new StreamingRunner()
                .run(key, restarted, repository, new StreamingRunner.Settings(1000, 1, 1, true))
            != 3
        || !second.toString().isEmpty()) throw new AssertionError(second);
    // Actual FakeSource reader+enumerator and Console writer state survive a mid-input failure.
    class FailBeforeThirdLine extends StringWriter {
      int lines;

      @Override
      public void write(String value) {
        if (lines == 2) throw new IllegalStateException("output rejected before third line");
        super.write(value);
        lines++;
      }
    }
    var prefix = new FailBeforeThirdLine();
    UUID resumeKey = UUID.randomUUID();
    var resumeRepository = new StreamingContracts.Repository(new ArrayList<>());
    var partial =
        new StreamingRunner.Pipeline<>(
            "resume-fake-v1",
            new FakeSource(3, 10, 1),
            new ConsoleSink(prefix),
            FakeSource.ReaderState.class,
            FakeSource.EnumeratorState.class,
            ConsoleSink.State.class,
            String.class);
    var ticks = new java.util.concurrent.atomic.AtomicLong();
    try {
      new StreamingRunner(() -> ticks.getAndAdd(2000000))
          .run(resumeKey, partial, resumeRepository, new StreamingRunner.Settings(1, 1, 1));
      throw new AssertionError("Expected output failure");
    } catch (IllegalStateException expected) {
      if (!expected.getMessage().contains("third line")) throw expected;
    }
    var suffix = new StringWriter();
    var resumed =
        new StreamingRunner.Pipeline<>(
            "resume-fake-v1",
            new FakeSource(3, 10, 1),
            new ConsoleSink(suffix),
            FakeSource.ReaderState.class,
            FakeSource.EnumeratorState.class,
            ConsoleSink.State.class,
            String.class);
    if (new StreamingRunner()
                .run(
                    resumeKey,
                    resumed,
                    resumeRepository,
                    new StreamingRunner.Settings(1000, 1, 1, true))
            != 3
        || !prefix.toString().equals("[10,\"row-10\"]\n[11,\"row-11\"]\n")
        || !suffix.toString().equals("[12,\"row-12\"]\n"))
      throw new AssertionError(prefix + " / " + suffix);
    System.out.println(
        "PASS real FakeSource -> streaming/checkpoint bridge -> ConsoleSink, terminal restore"
            + " without duplicate output; serialized TEST repository (NOT Hazelcast proof)");
  }
}
