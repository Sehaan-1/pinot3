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

import java.util.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.table.CatalogTable;

/** Single-reader bounded explicit local files. Checkpoint and restore intentionally unsupported. */
public final class FileSource
    implements SeaTunnelSource<FileSource.Split, FileSource.State, FileSource.State> {
  final FileConfig config;

  FileSource(FileConfig config) {
    this.config = config;
  }

  public record Split(String path, long size, String modified, String key) implements SourceSplit {
    public Split {
      FileConfig.child(path);
      if (size < 0) throw new IllegalArgumentException("Negative file size");
      Objects.requireNonNull(modified);
      Objects.requireNonNull(key);
    }

    public String splitId() {
      return path;
    }
  }

  public record State() implements CheckpointState {}

  static UnsupportedOperationException unsupported() {
    return new UnsupportedOperationException(
        "Local file checkpoint/restore is unsupported; rerun bounded inputs explicitly");
  }

  public CatalogTable producedTable() {
    return config.table;
  }

  public Boundedness boundedness() {
    return Boundedness.BOUNDED;
  }

  public SourceReader<Split, State> createReader(int id, Optional<State> restored) {
    if (id != 0) throw new IllegalArgumentException("File connector requires reader0");
    if (restored.isPresent()) throw unsupported();
    return new Reader();
  }

  public SplitEnumerator<Split, State> createEnumerator(
      SplitEnumerator.Context<Split> context, Optional<State> restored) {
    if (restored.isPresent()) throw unsupported();
    if (context.currentParallelism() != 1)
      throw new IllegalArgumentException("File connector requires parallelism1");
    return new SplitEnumerator<>() {
      boolean started, closed, registered, assigned;
      List<Split> splits;

      private void active() {
        if (!started || closed) throw new IllegalStateException("File enumerator inactive");
      }

      private void check(int id) {
        active();
        if (id != 0) throw new IllegalArgumentException("Unknown file reader");
      }

      public void start() throws Exception {
        if (started || closed) throw new IllegalStateException("Cannot restart file enumerator");
        started = true;
        try {
          var values = new ArrayList<Split>();
          for (String path : config.paths) {
            interrupt();
            values.add(FilePaths.stat(config, path));
          }
          splits = List.copyOf(values);
        } catch (InterruptedException e) {
          close();
          throw e;
        } catch (Exception e) {
          close();
          throw FileConfig.failure();
        }
      }

      public void registerReader(int id) {
        check(id);
        registered = true;
      }

      public void handleSplitRequest(int id) throws Exception {
        check(id);
        if (!registered) throw new IllegalStateException("Reader not registered");
        if (!assigned) {
          context.assignSplits(0, splits);
          assigned = true;
        }
        context.signalNoMoreSplits(0);
      }

      public void addSplitsBack(List<Split> returned, int id) {
        throw unsupported();
      }

      public State snapshotState(long id) {
        throw unsupported();
      }

      public void notifyCheckpointComplete(long id) {
        throw unsupported();
      }

      public void notifyCheckpointAborted(long id) {
        throw unsupported();
      }

      public void close() {
        closed = true;
        splits = null;
      }
    };
  }

  static void interrupt() throws InterruptedException {
    if (Thread.currentThread().isInterrupted())
      throw new InterruptedException("Local file operation interrupted");
  }

  private final class Reader implements SourceReader<Split, State> {
    boolean started, closed, noMore;
    final Deque<Split> pending = new ArrayDeque<>();
    final Set<String> accepted = new HashSet<>();
    Split current;
    FileCodec.Reader input;

    void active() {
      if (!started || closed) throw new IllegalStateException("File reader inactive");
    }

    public void start() {
      if (started || closed) throw new IllegalStateException("Cannot restart file reader");
      started = true;
    }

    public void addSplits(List<Split> splits) {
      active();
      if (noMore) throw new IllegalStateException("File assignment already complete");
      var seen = new HashSet<>(accepted);
      for (Split s : List.copyOf(splits))
        if (!config.paths.contains(s.path()) || s.size() > config.maxFile || !seen.add(s.path()))
          throw new IllegalArgumentException("Duplicate/foreign file split");
      pending.addAll(splits);
      accepted.addAll(seen);
    }

    public void notifyNoMoreSplits() {
      active();
      if (!accepted.equals(new HashSet<>(config.paths)))
        throw new IllegalStateException("Incomplete file assignment");
      noMore = true;
    }

    public ReadStatus pollNext(Output output) throws Exception {
      active();
      try {
        interrupt();
        while (true) {
          if (input == null) {
            current = pending.pollFirst();
            if (current == null)
              return noMore ? ReadStatus.END_OF_INPUT : ReadStatus.NOTHING_AVAILABLE;
            FilePaths.unchanged(config, current);
            input = FileCodec.reader(config, FilePaths.resolve(config, current.path()));
          }
          var row = input.next();
          if (row == null) {
            input.close();
            input = null;
            FilePaths.unchanged(config, current);
            current = null;
            continue;
          }
          FileRows.validate(config, row);
          interrupt();
          output.collect(row);
          return ReadStatus.MORE_AVAILABLE;
        }
      } catch (InterruptedException e) {
        close();
        Thread.currentThread().interrupt();
        throw e;
      } catch (Exception e) {
        close();
        throw FileConfig.failure();
      }
    }

    public State snapshotState(long id) {
      throw unsupported();
    }

    public void notifyCheckpointComplete(long id) {
      throw unsupported();
    }

    public void notifyCheckpointAborted(long id) {
      throw unsupported();
    }

    public void close() {
      if (closed) return;
      closed = true;
      pending.clear();
      if (input != null)
        try {
          input.close();
        } catch (Exception ignored) {
          /* Redacted; ownership released best effort. */
        } finally {
          input = null;
        }
    }
  }
}
