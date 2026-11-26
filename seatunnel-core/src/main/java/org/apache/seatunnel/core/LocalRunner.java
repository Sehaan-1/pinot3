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
package org.apache.seatunnel.core;

import java.io.Serializable;
import java.io.Writer;
import java.util.List;
import java.util.Optional;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SplitEnumerator;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.plugin.PluginContext;
import org.apache.seatunnel.plugin.PluginDiscovery;
import org.apache.seatunnel.plugin.SinkPlugin;
import org.apache.seatunnel.plugin.SourcePlugin;

/** Bounded, single-threaded execution only; no scheduling or checkpoint coordinator. */
public final class LocalRunner {
  public long run(JobConfig config, Writer output) throws Exception {
    try (var sources = PluginDiscovery.fromClasspath(SourcePlugin.class);
        var sinks = PluginDiscovery.fromClasspath(SinkPlugin.class)) {
      var source = sources.get(config.sourceId()).createSource(config.sourceOptions());
      var sink =
          sinks
              .get(config.sinkId())
              .createSink(config.sinkOptions(), source.producedTable(), new PluginContext(output));
      return execute(source, sink);
    }
  }

  /** Runs configured factories and closes every created operator, including on partial startup. */
  public <
          S extends SourceSplit,
          R extends CheckpointState,
          E extends CheckpointState,
          W extends CheckpointState,
          T extends Serializable>
      long execute(SeaTunnelSource<S, R, E> source, SeaTunnelSink<W, T> sink) throws Exception {
    if (source.boundedness() != Boundedness.BOUNDED) {
      throw new IllegalArgumentException("Local baseline requires a bounded source");
    }
    int arity = source.producedTable().columns().size();
    try (SinkCommitter<T> committer = sink.createCommitter().orElse(null)) {
      if (committer != null) {
        throw new IllegalArgumentException("Local baseline does not support transactional sinks");
      }
      try (SourceReader<S, R> reader = source.createReader(0, Optional.empty());
          SinkWriter<W, T> writer = sink.createWriter(0, Optional.empty())) {
        SplitEnumerator.Context<S> context =
            new SplitEnumerator.Context<>() {
              @Override
              public int currentParallelism() {
                return 1;
              }

              @Override
              public void assignSplits(int readerId, List<S> splits) throws Exception {
                checkReader(readerId);
                reader.addSplits(splits);
              }

              @Override
              public void signalNoMoreSplits(int readerId) throws Exception {
                checkReader(readerId);
                reader.notifyNoMoreSplits();
              }

              private void checkReader(int readerId) {
                if (readerId != 0) {
                  throw new IllegalArgumentException("Unknown local reader: " + readerId);
                }
              }
            };
        try (SplitEnumerator<S, E> enumerator =
            source.createEnumerator(context, Optional.empty())) {
          reader.start();
          enumerator.start();
          enumerator.registerReader(0);
          enumerator.handleSplitRequest(0);
          long[] count = {0};
          while (true) {
            if (Thread.currentThread().isInterrupted()) {
              throw new InterruptedException("Local job interrupted");
            }
            SourceReader.ReadStatus status =
                reader.pollNext(
                    row -> {
                      if (row.getArity() != arity) {
                        throw new IllegalArgumentException(
                            "Source row does not match declared arity");
                      }
                      writer.write(row);
                      count[0]++;
                    });
            if (status == SourceReader.ReadStatus.END_OF_INPUT) {
              writer.flush(true);
              return count[0];
            }
            if (status == SourceReader.ReadStatus.NOTHING_AVAILABLE) {
              enumerator.handleSplitRequest(0);
              Thread.sleep(1); // JDK backoff, not a scheduler or busy-spin.
            }
          }
        }
      }
    }
  }
}
