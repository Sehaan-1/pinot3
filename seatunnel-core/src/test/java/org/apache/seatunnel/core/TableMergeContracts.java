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

import java.util.*;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.*;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.config.Config;

/** Same executable contracts run by JUnit and the offline JDK smoke driver. */
final class TableMergeContracts {
  static int checks;

  static void check(boolean value) {
    checks++;
    if (!value) throw new AssertionError("check " + checks);
  }

  interface Work {
    void run() throws Exception;
  }

  static void rejects(Work work) throws Exception {
    checks++;
    try {
      work.run();
    } catch (Exception expected) {
      return;
    }
    throw new AssertionError("Expected rejection " + checks);
  }

  record State() implements CheckpointState {}

  record Split() implements SourceSplit {
    public String splitId() {
      return "one";
    }
  }

  static final CatalogTable TABLE =
      new CatalogTable(
          "first", List.of(new CatalogTable.Column("id", CatalogTable.DataType.INT, false)));
  static final List<String> events = new ArrayList<>();

  static final class Input implements SeaTunnelSource<Split, State, State> {
    final String name;
    final List<SeaTunnelRow> rows;
    CatalogTable table = TABLE;
    Boundedness bounded = Boundedness.BOUNDED;
    String fail = "";

    Input(String name, SeaTunnelRow... rows) {
      this.name = name;
      this.rows = List.of(rows);
    }

    void event(String stage) {
      events.add(name + ":" + stage);
      if (fail.equals(stage)) throw new IllegalStateException(stage);
    }

    public CatalogTable producedTable() {
      return table;
    }

    public Boundedness boundedness() {
      return bounded;
    }

    public SourceReader<Split, State> createReader(int id, Optional<State> restored) {
      event("reader");
      return new SourceReader<>() {
        int index;
        boolean assigned;
        boolean waited;

        public void start() {
          event("reader-start");
        }

        public void addSplits(List<Split> splits) {
          assigned = true;
        }

        public void notifyNoMoreSplits() {}

        public ReadStatus pollNext(Output output) throws Exception {
          event("poll");
          check(assigned);
          if (!waited) {
            waited = true;
            return ReadStatus.NOTHING_AVAILABLE;
          }
          if (index == rows.size()) return ReadStatus.END_OF_INPUT;
          output.collect(rows.get(index++));
          return ReadStatus.MORE_AVAILABLE;
        }

        public void close() {
          event("reader-close");
        }

        public State snapshotState(long id) {
          throw new UnsupportedOperationException();
        }

        public void notifyCheckpointComplete(long id) {}

        public void notifyCheckpointAborted(long id) {}
      };
    }

    public SplitEnumerator<Split, State> createEnumerator(
        SplitEnumerator.Context<Split> context, Optional<State> restored) {
      event("enumerator");
      return new SplitEnumerator<>() {
        boolean assigned;

        public void start() {
          event("enumerator-start");
        }

        public void registerReader(int id) {}

        public void handleSplitRequest(int id) throws Exception {
          if (!assigned) {
            context.assignSplits(id, List.of(new Split()));
            assigned = true;
            context.signalNoMoreSplits(id);
          }
        }

        public void addSplitsBack(List<Split> splits, int id) {
          throw new UnsupportedOperationException();
        }

        public void close() {
          event("enumerator-close");
        }

        public State snapshotState(long id) {
          throw new UnsupportedOperationException();
        }

        public void notifyCheckpointComplete(long id) {}

        public void notifyCheckpointAborted(long id) {}
      };
    }
  }

  static final class Sink implements SeaTunnelSink<State, String> {
    List<SeaTunnelRow> rows = new ArrayList<>();
    boolean fail;

    public Optional<SinkCommitter<String>> createCommitter() {
      return Optional.empty();
    }

    public SinkWriter<State, String> createWriter(int id, Optional<State> state) {
      events.add("writer");
      return new SinkWriter<>() {
        public void write(SeaTunnelRow row) {
          if (fail) throw new IllegalStateException("write");
          rows.add(row);
        }

        public void flush(boolean end) {
          check(end);
          events.add("flush");
        }

        public void close() {
          events.add("writer-close");
        }

        public List<String> prepareCommit(long id) {
          throw new UnsupportedOperationException();
        }

        public State snapshotState(long id) {
          throw new UnsupportedOperationException();
        }

        public void notifyCheckpointComplete(long id) {}

        public void notifyCheckpointAborted(long id) {}
      };
    }
  }

  public static void main(String[] args) throws Exception {
    checks = 0;
    var runner = new LocalRunner();
    for (RowKind kind : RowKind.values()) {
      events.clear();
      var row = SeaTunnelRow.ofKind(kind, 1);
      var first = new Input("a", row, row);
      var empty = new Input("empty");
      var last = new Input("b", row);
      last.table = new CatalogTable("other-name", TABLE.columns());
      var sink = new Sink();
      check(runner.executeTableMerge(List.of(first, empty, last), sink) == 3);
      check(
          sink.rows.size() == 3
              && sink.rows.stream().allMatch(r -> r == row && r.getRowKind() == kind));
      check(
          Collections.frequency(events, "writer") == 1
              && Collections.frequency(events, "flush") == 1);
      check(events.indexOf("a:reader-close") < events.indexOf("empty:reader"));
      check(events.indexOf("empty:reader-close") < events.indexOf("b:reader"));
      check(events.indexOf("b:reader-close") < events.indexOf("flush"));
      check(events.get(events.size() - 1).equals("writer-close"));
    }
    for (String failure :
        List.of(
            "reader",
            "reader-start",
            "enumerator",
            "enumerator-start",
            "poll",
            "enumerator-close",
            "reader-close",
            "write")) {
      events.clear();
      var input = new Input("a", new SeaTunnelRow(new Object[] {1}));
      var sink = new Sink();
      if (failure.equals("write")) sink.fail = true;
      else input.fail = failure;
      rejects(() -> runner.executeTableMerge(List.of(input, new Input("later")), sink));
      check(
          !events.contains("later:reader")
              && !events.contains("flush")
              && events.contains("writer-close"));
      if (!failure.equals("reader")) check(events.contains("a:reader-close"));
      if (List.of(
              "reader-start",
              "enumerator-start",
              "poll",
              "enumerator-close",
              "reader-close",
              "write")
          .contains(failure)) check(events.contains("a:enumerator-close"));
    }
    for (String bad : List.of("type", "name", "nullability", "unbounded")) {
      events.clear();
      var a = new Input("a");
      var b = new Input("b");
      if (bad.equals("unbounded")) b.bounded = Boundedness.UNBOUNDED;
      else
        b.table =
            new CatalogTable(
                "b",
                List.of(
                    new CatalogTable.Column(
                        bad.equals("name") ? "other" : "id",
                        bad.equals("type") ? CatalogTable.DataType.LONG : CatalogTable.DataType.INT,
                        bad.equals("nullability"))));
      rejects(() -> runner.executeTableMerge(List.of(a, b), new Sink()));
      check(events.isEmpty());
    }
    rejects(() -> runner.executeTableMerge(List.of(), new Sink()));
    rejects(() -> runner.executeTableMerge(Collections.nCopies(33, new Input("a")), new Sink()));
    events.clear();
    Thread.currentThread().interrupt();
    try {
      rejects(
          () -> runner.executeTableMerge(List.of(new Input("a"), new Input("later")), new Sink()));
    } finally {
      Thread.interrupted();
    }
    check(
        events.contains("a:reader-close")
            && events.contains("a:enumerator-close")
            && !events.contains("later:reader")
            && !events.contains("flush"));
    events.clear();
    var secondFailure = new Input("bad", new SeaTunnelRow(1, 2));
    var partial = new Sink();
    rejects(
        () ->
            runner.executeTableMerge(
                List.of(new Input("ok", new SeaTunnelRow(9)), secondFailure, new Input("later")),
                partial));
    check(
        partial.rows.size() == 1 && !events.contains("flush") && !events.contains("later:reader"));
    check(
        events.contains("ok:reader-close")
            && events.contains("bad:reader-close")
            && events.contains("bad:enumerator-close")
            && events.contains("writer-close"));
    events.clear();
    check(runner.executeTableMerge(Collections.nCopies(32, new Input("empty")), new Sink()) == 0);
    check(
        Collections.frequency(events, "flush") == 1
            && Collections.frequency(events, "empty:reader-close") == 32);
    var mutable = new HashMap<String, Object>();
    mutable.put("rows", 2);
    var entry = new JobConfig.SourceConfig("fake", mutable);
    var extra = new ArrayList<JobConfig.SourceConfig>();
    extra.add(entry);
    var owned = new JobConfig("fake", Map.of(), "console", Map.of(), List.of(), extra);
    mutable.clear();
    extra.clear();
    check(owned.sources().size() == 2 && owned.sources().get(1).options().get("rows").equals(2));
    rejects(() -> owned.additionalSources().clear());
    rejects(() -> entry.options().clear());
    String sink = "\nsink.plugin=console";
    var legacy = JobConfig.parse(Config.parseString("source.plugin=fake" + sink));
    check(legacy.sources().size() == 1);
    var parsed =
        JobConfig.parse(
            Config.parseString(
                "sources=[{plugin=fake,options.rows=0},{plugin=fake,options.rows=2}]" + sink));
    check(parsed.sources().size() == 2 && parsed.sources().get(1).options().get("rows").equals(2));
    rejects(() -> parsed.sources().clear());
    for (String text :
        List.of(
            "",
            "sources=[]",
            "sources=[{plugin=fake}]",
            "sources=null",
            "sources={}",
            "sources=[{plugin=BAD}, {plugin=fake}]",
            "sources=[{plugin=fake,unknown=1},{plugin=fake}]",
            "source.plugin=fake\nsources=[{plugin=fake},{plugin=fake}]",
            "sources=" + Collections.nCopies(33, "{plugin=fake}").toString()))
      rejects(() -> JobConfig.parse(Config.parseString(text + sink)));
    System.out.println("PASS TableMerge contracts: " + checks + " assertions");
  }
}
