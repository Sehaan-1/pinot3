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
package org.apache.seatunnel.connector.file;

import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Bounded staged output. Atomic hard-link publication is no-clobber, not a transaction. */
public final class FileSink implements SeaTunnelSink<FileSink.State, String> {
  final FileConfig config;

  FileSink(FileConfig config) {
    this.config = config;
  }

  public record State() implements CheckpointState {}

  public Optional<SinkCommitter<String>> createCommitter() {
    return Optional.empty();
  }

  public SinkWriter<State, String> createWriter(int id, Optional<State> restored) throws Exception {
    if (id != 0) throw new IllegalArgumentException("File connector requires writer0");
    if (restored.isPresent()) throw FileSource.unsupported();
    return new Writer();
  }

  private final class Writer implements SinkWriter<State, String> {
    Path target, stage;
    FileCodec.Writer output;
    boolean closed, published;

    Writer() throws Exception {
      try {
        FileSource.interrupt();
        target = FilePaths.resolve(config, config.paths.get(0));
        if (Files.exists(target, LinkOption.NOFOLLOW_LINKS)) throw FileConfig.failure();
        stage = Files.createTempFile(target.getParent(), ".seatunnel-", ".stage");
        output = FileCodec.writer(config, stage);
      } catch (InterruptedException e) {
        close();
        throw e;
      } catch (Exception e) {
        close();
        throw FileConfig.failure();
      }
    }

    void active() {
      if (closed || published)
        throw new IllegalStateException("File writer is closed or finalized");
    }

    void size() throws Exception {
      if (Files.size(stage) > config.maxFile) throw FileConfig.failure();
    }

    public void write(SeaTunnelRow row) throws Exception {
      active();
      try {
        FileSource.interrupt();
        FileRows.validate(config, row);
        output.write(row);
        size();
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        throw FileConfig.failure();
      }
    }

    public void flush(boolean end) throws Exception {
      if (published && end && !closed) return;
      active();
      try {
        FileSource.interrupt();
        if (!end) {
          output.flush();
          size();
          return;
        }
        var owned = output;
        output = null;
        owned.close();
        size();
        try (var channel =
            FileChannel.open(stage, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS)) {
          channel.force(true);
        }
        if (!target.equals(FilePaths.resolve(config, config.paths.get(0))))
          throw FileConfig.failure();
        FileSource.interrupt();
        Files.createLink(target, stage);
        published = true;
        Files.delete(stage);
        stage = null;
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        throw FileConfig.failure();
      }
    }

    public List<String> prepareCommit(long id) {
      throw FileSource.unsupported();
    }

    public State snapshotState(long id) {
      throw FileSource.unsupported();
    }

    public void notifyCheckpointComplete(long id) {
      throw FileSource.unsupported();
    }

    public void notifyCheckpointAborted(long id) {
      throw FileSource.unsupported();
    }

    public void close() {
      if (closed) return;
      closed = true;
      if (output != null)
        try {
          output.close();
        } catch (Exception ignored) {
          /* May leave staging after process/device failure. */
        } finally {
          output = null;
        }
      if (stage != null)
        try {
          Files.deleteIfExists(stage);
        } catch (Exception ignored) {
          /* Never delete a published target. */
        } finally {
          stage = null;
        }
    }
  }
}
