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
package org.apache.seatunnel.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SplitEnumerator;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.connector.console.ConsoleSink;
import org.apache.seatunnel.connector.fake.FakeSource;
import org.apache.seatunnel.connector.fake.FakeSource.EnumeratorState;
import org.apache.seatunnel.connector.fake.FakeSource.ReaderState;
import org.apache.seatunnel.connector.fake.FakeSource.Split;
import org.apache.seatunnel.core.LocalRunner;
import org.junit.jupiter.api.Test;

/** Fault-injecting forwarding adapters retain the real FakeSource and ConsoleSink code paths. */
class LocalRunnerTest {
  @Test
  void closesEveryResourceInReverseOrderOnSuccess() throws Exception {
    Fixture fixture = new Fixture();
    assertThat(new LocalRunner().execute(fixture.source(), fixture.sink())).isEqualTo(2);
    assertThat(fixture.closed).containsExactly("enumerator", "writer", "reader");
    assertThat(fixture.output.toString()).contains("row-0", "row-1");
  }

  @Test
  void preservesPrimaryWriteFailureAndSuppressedCleanupFailure() {
    Fixture fixture = new Fixture();
    fixture.failWrite = true;
    fixture.failClose = true;
    assertThatThrownBy(() -> new LocalRunner().execute(fixture.source(), fixture.sink()))
        .isInstanceOf(IOException.class).hasMessage("write failed")
        .satisfies(error -> assertThat(error.getSuppressed()).hasSize(1));
    assertThat(fixture.closed).containsExactly("enumerator", "writer", "reader");
  }

  @Test
  void closesAlreadyCreatedResourcesWhenFactoryCreationFails() {
    Fixture fixture = new Fixture();
    fixture.failEnumerator = true;
    assertThatThrownBy(() -> new LocalRunner().execute(fixture.source(), fixture.sink()))
        .isInstanceOf(IOException.class).hasMessage("enumerator creation failed");
    assertThat(fixture.closed).containsExactly("writer", "reader");
  }

  @Test
  void honorsInterruptionAndStillClosesResources() {
    Fixture fixture = new Fixture();
    Thread.currentThread().interrupt();
    try {
      assertThatThrownBy(() -> new LocalRunner().execute(fixture.source(), fixture.sink()))
          .isInstanceOf(InterruptedException.class);
      assertThat(fixture.closed).containsExactly("enumerator", "writer", "reader");
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      Thread.interrupted();
    }
  }

  private static final class Fixture {
    final FakeSource fake = new FakeSource(2, 0, 1);
    final StringWriter output = new StringWriter();
    final List<String> closed = new ArrayList<>();
    boolean failWrite;
    boolean failClose;
    boolean failEnumerator;

    SeaTunnelSource<Split, ReaderState, EnumeratorState> source() {
      return new SeaTunnelSource<>() {
        @Override
        public CatalogTable producedTable() { return fake.producedTable(); }

        @Override
        public Boundedness boundedness() { return fake.boundedness(); }

        @Override
        public SourceReader<Split, ReaderState> createReader(int id, Optional<ReaderState> state) {
          var delegate = fake.createReader(id, state);
          return new SourceReader<>() {
            @Override
            public void start() throws Exception { delegate.start(); }
            @Override
            public void addSplits(List<Split> splits) throws Exception { delegate.addSplits(splits); }
            @Override
            public void notifyNoMoreSplits() throws Exception { delegate.notifyNoMoreSplits(); }
            @Override
            public ReadStatus pollNext(Output output) throws Exception { return delegate.pollNext(output); }
            @Override
            public ReaderState snapshotState(long id) throws Exception { return delegate.snapshotState(id); }
            @Override
            public void notifyCheckpointComplete(long id) throws Exception { delegate.notifyCheckpointComplete(id); }
            @Override
            public void notifyCheckpointAborted(long id) throws Exception { delegate.notifyCheckpointAborted(id); }
            @Override
            public void close() throws Exception { closed.add("reader"); delegate.close(); }
          };
        }

        @Override
        public SplitEnumerator<Split, EnumeratorState> createEnumerator(
            SplitEnumerator.Context<Split> context, Optional<EnumeratorState> state) throws Exception {
          if (failEnumerator) { throw new IOException("enumerator creation failed"); }
          var delegate = fake.createEnumerator(context, state);
          return new SplitEnumerator<>() {
            @Override
            public void start() throws Exception { delegate.start(); }
            @Override
            public void registerReader(int id) throws Exception { delegate.registerReader(id); }
            @Override
            public void handleSplitRequest(int id) throws Exception { delegate.handleSplitRequest(id); }
            @Override
            public void addSplitsBack(List<Split> splits, int id) throws Exception { delegate.addSplitsBack(splits, id); }
            @Override
            public EnumeratorState snapshotState(long id) throws Exception { return delegate.snapshotState(id); }
            @Override
            public void notifyCheckpointComplete(long id) throws Exception { delegate.notifyCheckpointComplete(id); }
            @Override
            public void notifyCheckpointAborted(long id) throws Exception { delegate.notifyCheckpointAborted(id); }
            @Override
            public void close() throws Exception { closed.add("enumerator"); delegate.close(); }
          };
        }
      };
    }

    SeaTunnelSink<ConsoleSink.State, String> sink() {
      ConsoleSink console = new ConsoleSink(output);
      return new SeaTunnelSink<>() {
        @Override
        public Optional<SinkCommitter<String>> createCommitter() { return console.createCommitter(); }

        @Override
        public SinkWriter<ConsoleSink.State, String> createWriter(int id, Optional<ConsoleSink.State> state) {
          var delegate = console.createWriter(id, state);
          return new SinkWriter<>() {
            @Override
            public void write(SeaTunnelRow row) throws Exception {
              if (failWrite) { throw new IOException("write failed"); }
              delegate.write(row);
            }
            @Override
            public void flush(boolean end) throws Exception { delegate.flush(end); }
            @Override
            public List<String> prepareCommit(long id) throws Exception { return delegate.prepareCommit(id); }
            @Override
            public ConsoleSink.State snapshotState(long id) throws Exception { return delegate.snapshotState(id); }
            @Override
            public void notifyCheckpointComplete(long id) throws Exception { delegate.notifyCheckpointComplete(id); }
            @Override
            public void notifyCheckpointAborted(long id) throws Exception { delegate.notifyCheckpointAborted(id); }
            @Override
            public void close() throws Exception {
              closed.add("writer");
              delegate.close();
              if (failClose) { throw new IOException("close failed"); }
            }
          };
        }
      };
    }
  }
}
