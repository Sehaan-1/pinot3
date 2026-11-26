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
package org.apache.seatunnel.connector.console;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.Optional;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.plugin.PluginContext;
import org.junit.jupiter.api.Test;

class ConsoleSinkTest {
  @Test
  void writesJsonLinesFlushesAndDoesNotCloseCallerOutput() throws Exception {
    Output output = new Output();
    ConsoleSink sink = new ConsoleSink(output);
    assertThat(sink.createCommitter()).isEmpty();
    var writer = sink.createWriter(0, Optional.of(new ConsoleSink.State(5)));
    writer.write(new SeaTunnelRow(10L, "row-10"));
    writer.write(new SeaTunnelRow("a\nb", null));
    assertThat(writer.snapshotState(1).written()).isEqualTo(7);
    assertThat(writer.prepareCommit(1)).isEmpty();
    writer.notifyCheckpointComplete(1);
    writer.notifyCheckpointAborted(1);
    writer.flush(true);
    writer.close();
    writer.close();
    assertThat(output.text.toString()).isEqualTo("[10,\"row-10\"]\n[\"a\\nb\",null]\n");
    assertThat(output.flushed).isTrue();
    assertThat(output.closed).isFalse();
    assertThatThrownBy(() -> writer.write(new SeaTunnelRow(1)))
        .isInstanceOf(IllegalStateException.class);
  }

  @Test
  void propagatesOutputFailuresAndDoesNotAdvanceTheCount() throws Exception {
    Output output = new Output();
    var writer = new ConsoleSink(output).createWriter(0, Optional.empty());
    output.failWrite = true;
    assertThatThrownBy(() -> writer.write(new SeaTunnelRow(1))).isInstanceOf(IOException.class);
    assertThat(writer.snapshotState(0).written()).isZero();
    output.failFlush = true;
    assertThatThrownBy(writer::close).isInstanceOf(IOException.class);
    writer.close();
    assertThat(output.closed).isFalse();
  }

  @Test
  void validatesFactoryAndCheckpointArguments() throws Exception {
    assertThat(new ConsoleSinkPlugin().identifier()).isEqualTo("console");
    assertThatThrownBy(
            () ->
                new ConsoleSinkPlugin()
                    .createSink(Map.of("typo", true), null, new PluginContext(new Output())))
        .isInstanceOf(org.apache.seatunnel.common.exception.ConfigException.class);
    assertThatThrownBy(() -> new ConsoleSink.State(-1))
        .isInstanceOf(IllegalArgumentException.class);
    try (var writer = new ConsoleSink(new Output()).createWriter(0, Optional.empty())) {
      assertThatThrownBy(() -> writer.snapshotState(-1))
          .isInstanceOf(IllegalArgumentException.class);
    }
  }

  private static final class Output extends Writer {
    final StringBuilder text = new StringBuilder();
    boolean flushed;
    boolean closed;
    boolean failWrite;
    boolean failFlush;

    @Override
    public void write(char[] value, int offset, int length) throws IOException {
      if (failWrite) {
        throw new IOException("write failed");
      }
      text.append(value, offset, length);
    }

    @Override
    public void flush() throws IOException {
      if (failFlush) {
        throw new IOException("flush failed");
      }
      flushed = true;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
