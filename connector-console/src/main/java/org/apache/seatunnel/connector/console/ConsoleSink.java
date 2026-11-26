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

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.format.JsonFormat;

/** Non-transactional JSON-lines sink. The caller owns the output Writer. */
public final class ConsoleSink implements SeaTunnelSink<ConsoleSink.State, String> {
  private final Writer output;

  public ConsoleSink(Writer output) {
    this.output = Objects.requireNonNull(output, "output");
  }

  public record State(long written) implements CheckpointState {
    public State {
      if (written < 0) {
        throw new IllegalArgumentException("written must be nonnegative");
      }
    }
  }

  @Override
  public SinkWriter<State, String> createWriter(int writerId, Optional<State> restored) {
    if (writerId < 0) {
      throw new IllegalArgumentException("writerId must be nonnegative");
    }
    return new ConsoleWriter(restored.orElse(new State(0)).written());
  }

  @Override
  public Optional<SinkCommitter<String>> createCommitter() {
    return Optional.empty(); // Console output cannot be committed or rolled back.
  }

  private final class ConsoleWriter implements SinkWriter<State, String> {
    private final JsonFormat<Object[]> format = new JsonFormat<>(Object[].class);
    private long written;
    private boolean closed;

    ConsoleWriter(long written) {
      this.written = written;
    }

    private void ensureOpen() {
      if (closed) {
        throw new IllegalStateException("Console writer is closed");
      }
    }

    @Override
    public void write(SeaTunnelRow row) throws IOException {
      ensureOpen();
      String line = new String(format.serialize(row.getFields()), StandardCharsets.UTF_8) + "\n";
      synchronized (output) {
        output.write(line);
      }
      written++;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
      ensureOpen();
      synchronized (output) {
        output.flush();
      }
    }

    private void checkpoint(long id) {
      ensureOpen();
      if (id < 0) {
        throw new IllegalArgumentException("checkpoint ID must be nonnegative");
      }
    }

    @Override
    public List<String> prepareCommit(long id) throws IOException {
      checkpoint(id);
      flush(false);
      return List.of(); // No transaction handles exist for stdout.
    }

    @Override
    public State snapshotState(long id) {
      checkpoint(id);
      return new State(written);
    }

    @Override
    public void notifyCheckpointComplete(long id) {
      checkpoint(id); // Already printed rows have no commit operation.
    }

    @Override
    public void notifyCheckpointAborted(long id) {
      checkpoint(id); // Console output is explicitly not reversible.
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        try {
          flush(false);
        } finally {
          closed = true;
        }
      }
    }
  }
}
