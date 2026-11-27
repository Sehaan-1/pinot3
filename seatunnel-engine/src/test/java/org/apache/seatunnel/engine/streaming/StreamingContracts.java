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
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.seatunnel.api.sink.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.state.*;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.engine.checkpoint.*;

/** Deterministic test-only repository and protocol fixture, NOT external transactional E2E. */
final class StreamingContracts {
  static int checks;

  static void check(boolean ok) {
    checks++;
    if (!ok) throw new AssertionError("check " + checks);
  }

  interface Work {
    void run() throws Exception;
  }

  static Exception reject(Work work) throws Exception {
    checks++;
    try {
      work.run();
    } catch (Exception expected) {
      return expected;
    }
    throw new AssertionError("Expected failure " + checks);
  }

  static final CatalogTable TABLE =
      new CatalogTable(
          "stream", List.of(new CatalogTable.Column("id", CatalogTable.DataType.LONG, false)));
  static final StreamingRunner.Settings SETTINGS = new StreamingRunner.Settings(1, 1, 1);

  record ReaderState(int next, boolean assigned, boolean done) implements CheckpointState {}

  record EnumState(boolean assigned) implements CheckpointState {}

  record WriterState(int written) implements CheckpointState {}

  record Split() implements SourceSplit {
    public String splitId() {
      return "stream";
    }
  }

  static final class Repository implements CheckpointRepository {
    CheckpointRecord record;
    final List<String> events;
    String fail = "";

    Repository(List<String> events) {
      this.events = events;
    }

    @SuppressWarnings("unchecked")
    static <T extends Serializable> T copy(T value) {
      try {
        var bytes = new ByteArrayOutputStream();
        try (var out = new ObjectOutputStream(bytes)) {
          out.writeObject(value);
        }
        try (var in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
          return (T) in.readObject();
        }
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }

    public CheckpointRecord get(UUID key) {
      return record == null ? null : copy(record);
    }

    public boolean publish(UUID key, CheckpointRecord expected, CheckpointRecord next) {
      events.add("publish:" + next.id());
      if (fail.equals("publish-before")) throw new IllegalStateException("publication lost before");
      if (fail.equals("conflict")) return false;
      record = copy(next);
      if (fail.equals("publish-after")) throw new IllegalStateException("publication ack lost");
      return true;
    }

    public void complete(UUID key, CheckpointRecord decision) {
      events.add("store-complete:" + decision.id());
      if (fail.equals("complete")) throw new IllegalStateException("completion lost");
      record = copy(decision.completed());
    }
  }

  static final class Environment {
    final List<String> events = Collections.synchronizedList(new ArrayList<>());
    final Map<String, List<Long>> staged = new HashMap<>();
    final Set<String> committed = new HashSet<>();
    final List<Long> output = new ArrayList<>();
    final AtomicLong nanos = new AtomicLong();
    final CountDownLatch polled = new CountDownLatch(1);
    String fail = "";
    boolean transactional = true, idle, multiple, wrongArity, ticking = true, commitAppliedThenFail;
    int size = 3;

    void event(String name) {
      events.add(name);
      if (fail.equals(name)) throw new IllegalStateException(name);
    }

    void tick() {
      if (ticking) nanos.addAndGet(2000000);
    }

    StreamingRunner runner() {
      return new StreamingRunner(nanos::get);
    }

    StreamingRunner.Pipeline<Split, ReaderState, EnumState, WriterState, String> pipeline() {
      return new StreamingRunner.Pipeline<>(
          "binding-v1",
          new Source(this),
          new Sink(this),
          ReaderState.class,
          EnumState.class,
          WriterState.class,
          String.class);
    }
  }

  abstract static class Operator<S extends CheckpointState> implements Checkpointed<S> {
    final Environment env;
    final String name;

    Operator(Environment env, String name) {
      this.env = env;
      this.name = name;
    }

    public void notifyCheckpointComplete(long id) {
      env.event(name + "-complete:" + id);
    }

    public void notifyCheckpointAborted(long id) {
      env.event(name + "-abort:" + id);
    }
  }

  static final class Source implements SeaTunnelSource<Split, ReaderState, EnumState> {
    final Environment env;

    Source(Environment env) {
      this.env = env;
    }

    public CatalogTable producedTable() {
      return TABLE;
    }

    public Boundedness boundedness() {
      return Boundedness.UNBOUNDED;
    }

    public SourceReader<Split, ReaderState> createReader(int id, Optional<ReaderState> state) {
      env.event("reader-create");
      return new Reader(env, state.orElse(new ReaderState(0, false, false)));
    }

    public SplitEnumerator<Split, EnumState> createEnumerator(
        SplitEnumerator.Context<Split> context, Optional<EnumState> state) {
      env.event("enum-create");
      return new Enumerator(env, context, state.orElse(new EnumState(false)));
    }
  }

  static final class Reader extends Operator<ReaderState>
      implements SourceReader<Split, ReaderState> {
    int next;
    boolean assigned, done;

    Reader(Environment env, ReaderState state) {
      super(env, "reader");
      next = state.next();
      assigned = state.assigned();
      done = state.done();
      env.event("reader-restore:" + next);
    }

    public void start() {
      env.event("reader-start");
    }

    public void addSplits(List<Split> splits) {
      env.event("assign");
      check(!assigned);
      assigned = true;
    }

    public void notifyNoMoreSplits() {
      done = true;
    }

    public ReadStatus pollNext(Output output) throws Exception {
      env.event("poll:" + next);
      env.polled.countDown();
      env.tick();
      if (!assigned) throw new AssertionError("Unassigned reader");
      if (env.idle) return ReadStatus.NOTHING_AVAILABLE;
      check(assigned);
      if (next == env.size) return ReadStatus.END_OF_INPUT;
      output.collect(
          env.wrongArity
              ? new SeaTunnelRow((long) next, "bad")
              : SeaTunnelRow.ofKind(RowKind.DELETE, (long) next));
      if (env.multiple) output.collect(new SeaTunnelRow(99L));
      next++;
      env.event("advance:" + next);
      return ReadStatus.MORE_AVAILABLE;
    }

    public ReaderState snapshotState(long id) {
      env.event("reader-snapshot:" + id);
      return new ReaderState(next, assigned, done);
    }

    public void close() {
      env.event("reader-close");
    }
  }

  static final class Enumerator extends Operator<EnumState>
      implements SplitEnumerator<Split, EnumState> {
    final Context<Split> context;
    boolean assigned;

    Enumerator(Environment env, Context<Split> context, EnumState state) {
      super(env, "enum");
      this.context = context;
      assigned = state.assigned();
    }

    public void start() {
      env.event("enum-start");
    }

    public void registerReader(int id) {
      env.event("register");
    }

    public void handleSplitRequest(int id) throws Exception {
      if (!assigned) {
        context.assignSplits(0, List.of(new Split()));
        assigned = true;
      }
      context.signalNoMoreSplits(0);
    }

    public void addSplitsBack(List<Split> splits, int id) {
      throw new UnsupportedOperationException();
    }

    public EnumState snapshotState(long id) {
      env.event("enum-snapshot:" + id);
      return new EnumState(assigned);
    }

    public void close() {
      env.event("enum-close");
    }
  }

  static final class Sink implements SeaTunnelSink<WriterState, String> {
    final Environment env;

    Sink(Environment env) {
      this.env = env;
    }

    public SinkWriter<WriterState, String> createWriter(int id, Optional<WriterState> state) {
      env.event("writer-create");
      return new Writer(env, state.orElse(new WriterState(0)));
    }

    public Optional<SinkCommitter<String>> createCommitter() {
      env.event("committer-create");
      if (!env.transactional) return Optional.empty();
      return Optional.of(
          new SinkCommitter<>() {
            public void commit(long id, List<String> handles) {
              env.event("commit:" + id);
              for (String handle : handles)
                if (!env.committed.contains(handle)) {
                  env.output.addAll(Objects.requireNonNull(env.staged.get(handle)));
                  env.committed.add(handle);
                  env.staged.remove(handle);
                }
              if (env.commitAppliedThenFail) {
                env.commitAppliedThenFail = false;
                throw new IllegalStateException("commit ack lost");
              }
            }

            public void abort(long id, List<String> handles) {
              env.event("abort:" + id);
              handles.forEach(env.staged::remove);
            }

            public void close() {
              env.event("committer-close");
            }
          });
    }
  }

  static final class Writer extends Operator<WriterState>
      implements SinkWriter<WriterState, String> {
    int written;
    final List<Long> pending = new ArrayList<>();

    Writer(Environment env, WriterState state) {
      super(env, "writer");
      written = state.written();
      env.event("writer-restore:" + written);
    }

    public void write(SeaTunnelRow row) {
      env.event("write:" + row.getField(0));
      check(row.getRowKind() == RowKind.DELETE);
      long value = (Long) row.getField(0);
      if (env.transactional) pending.add(value);
      else env.output.add(value);
      written++;
    }

    public void flush(boolean end) {
      env.event("flush:" + end);
    }

    public List<String> prepareCommit(long id) {
      if (env.transactional) {
        env.staged.put("tx-" + id, List.copyOf(pending));
        pending.clear();
      }
      env.event("prepare:" + id);
      return env.transactional ? List.of("tx-" + id) : List.of();
    }

    public WriterState snapshotState(long id) {
      env.event("writer-snapshot:" + id);
      return new WriterState(written);
    }

    public void notifyCheckpointAborted(long id) {
      env.staged.remove("tx-" + id);
      super.notifyCheckpointAborted(id);
    }

    public void close() {
      pending.clear();
      env.event("writer-close");
    }
  }

  public static void main(String[] args) throws Exception {
    checks = 0;
    UUID key = UUID.randomUUID();
    var env = new Environment();
    var store = new Repository(env.events);
    check(env.runner().run(key, env.pipeline(), store, SETTINGS) == 3);
    check(
        env.output.equals(List.of(0L, 1L, 2L))
            && store.record.status() == CheckpointRecord.Status.COMPLETED);
    check(
        ((StreamingRunner.Snapshot) store.record.participants().get("pipeline").state())
            .finished());
    for (int i = 1; i <= 3; i++) {
      var e = env.events;
      check(e.indexOf("write:" + (i - 1)) < e.indexOf("advance:" + i));
      check(e.indexOf("prepare:" + i) < e.indexOf("writer-snapshot:" + i));
      check(e.indexOf("writer-snapshot:" + i) < e.indexOf("enum-snapshot:" + i));
      check(e.indexOf("enum-snapshot:" + i) < e.indexOf("reader-snapshot:" + i));
      check(e.indexOf("reader-snapshot:" + i) < e.indexOf("publish:" + i));
      check(e.indexOf("publish:" + i) < e.indexOf("commit:" + i));
      check(e.indexOf("commit:" + i) < e.indexOf("writer-complete:" + i));
      check(e.indexOf("writer-complete:" + i) < e.indexOf("enum-complete:" + i));
      check(e.indexOf("enum-complete:" + i) < e.indexOf("reader-complete:" + i));
      check(e.indexOf("reader-complete:" + i) < e.indexOf("store-complete:" + i));
    }
    env.events.clear();
    check(env.runner().run(key, env.pipeline(), store, SETTINGS) == 3);
    check(!env.events.contains("reader-create") && !env.events.contains("writer-create"));
    for (String failure :
        List.of("prepare:1", "writer-snapshot:1", "enum-snapshot:1", "reader-snapshot:1")) {
      var f = new Environment();
      f.fail = failure;
      var repo = new Repository(f.events);
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(repo.record == null && f.staged.isEmpty() && f.events.contains("abort:1"));
      check(
          f.events.contains("writer-abort:1")
              && f.events.contains("enum-abort:1")
              && f.events.contains("reader-abort:1"));
      check(
          f.events.contains("reader-close")
              && f.events.contains("writer-close")
              && f.events.contains("committer-close"));
    }
    for (String failure : List.of("publish-before", "publish-after", "conflict", "complete")) {
      var f = new Environment();
      var repo = new Repository(f.events);
      repo.fail = failure;
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(
          f.events.stream()
              .noneMatch(e -> e.startsWith("abort:") || e.startsWith("reader-abort:")));
      if (failure.equals("publish-after") || failure.equals("complete")) {
        check(repo.record.status() == CheckpointRecord.Status.COMMITTING);
        repo.fail = "";
        f.events.clear();
        check(f.runner().run(key, f.pipeline(), repo, SETTINGS) == 3);
        check(f.events.indexOf("commit:1") < f.events.indexOf("reader-create"));
        check(f.events.indexOf("reader-complete:1") < f.events.indexOf("store-complete:1"));
        check(f.output.equals(List.of(0L, 1L, 2L)));
      } else check(repo.record == null);
    }
    for (String failure :
        List.of(
            "commit:1", "writer-complete:1", "enum-complete:1", "reader-complete:1", "poll:2")) {
      var f = new Environment();
      var repo = new Repository(f.events);
      f.fail = failure;
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(f.events.stream().noneMatch(e -> e.startsWith("abort:")));
      f.fail = "";
      f.events.clear();
      check(f.runner().run(key, f.pipeline(), repo, SETTINGS) == 3);
      check(f.output.equals(List.of(0L, 1L, 2L)));
    }
    var startup = new Environment();
    startup.fail = "commit:1";
    var startupRepo = new Repository(startup.events);
    reject(() -> startup.runner().run(key, startup.pipeline(), startupRepo, SETTINGS));
    startup.fail = "writer-create";
    reject(() -> startup.runner().run(key, startup.pipeline(), startupRepo, SETTINGS));
    check(startupRepo.record.status() == CheckpointRecord.Status.COMMITTING);
    startup.fail = "";
    check(startup.runner().run(key, startup.pipeline(), startupRepo, SETTINGS) == 3);
    check(startup.output.equals(List.of(0L, 1L, 2L)));
    var missing = new Environment();
    var missingRepo = new Repository(missing.events);
    reject(
        () ->
            missing
                .runner()
                .run(
                    key,
                    missing.pipeline(),
                    missingRepo,
                    new StreamingRunner.Settings(1, 1, 1, true)));
    check(!missing.events.contains("reader-create") && !missing.events.contains("writer-create"));
    // A commit may have applied while its acknowledgement was lost; handles must be idempotent.
    var ack = new Environment();
    ack.commitAppliedThenFail = true;
    var ackRepo = new Repository(ack.events);
    reject(() -> ack.runner().run(key, ack.pipeline(), ackRepo, SETTINGS));
    check(ack.output.equals(List.of(0L)));
    check(ack.runner().run(key, ack.pipeline(), ackRepo, SETTINGS) == 3);
    check(ack.output.equals(List.of(0L, 1L, 2L)));
    // Terminal COMMITTING also needs live completion notifications before marking COMPLETED.
    var terminal = new Environment();
    terminal.size = 0;
    terminal.fail = "reader-complete:1";
    var terminalRepo = new Repository(terminal.events);
    reject(() -> terminal.runner().run(key, terminal.pipeline(), terminalRepo, SETTINGS));
    check(terminalRepo.record.status() == CheckpointRecord.Status.COMMITTING);
    terminal.fail = "";
    terminal.events.clear();
    check(terminal.runner().run(key, terminal.pipeline(), terminalRepo, SETTINGS) == 0);
    check(
        terminal.events.indexOf("reader-complete:1") < terminal.events.indexOf("store-complete:1")
            && !terminal.events.contains("poll:0"));
    for (String failure :
        List.of(
            "reader-create",
            "writer-create",
            "enum-create",
            "reader-start",
            "enum-start",
            "register")) {
      var f = new Environment();
      f.fail = failure;
      var repo = new Repository(f.events);
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(f.events.contains("committer-close"));
      if (!failure.equals("reader-create")) check(f.events.contains("reader-close"));
      if (!List.of("reader-create", "writer-create").contains(failure))
        check(f.events.contains("writer-close"));
      if (List.of("reader-start", "enum-start", "register").contains(failure))
        check(f.events.contains("enum-close"));
    }
    for (String closeFailure :
        List.of("enum-close", "writer-close", "reader-close", "committer-close")) {
      var f = new Environment();
      f.fail = closeFailure;
      var repo = new Repository(f.events);
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(
          f.events.contains("enum-close")
              && f.events.contains("writer-close")
              && f.events.contains("reader-close")
              && f.events.contains("committer-close"));
      check(
          repo.record.status() == CheckpointRecord.Status.COMPLETED
              && ((StreamingRunner.Snapshot) repo.record.participants().get("pipeline").state())
                  .finished());
    }
    for (String bad : List.of("arity", "cap")) {
      var f = new Environment();
      f.wrongArity = bad.equals("arity");
      f.multiple = bad.equals("cap");
      var repo = new Repository(f.events);
      reject(() -> f.runner().run(key, f.pipeline(), repo, SETTINGS));
      check(repo.record == null && f.output.isEmpty() && f.events.contains("writer-close"));
    }
    var binding = new Environment();
    var wrong = env.pipeline();
    var changed =
        new StreamingRunner.Pipeline<>(
            "other-version",
            wrong.source(),
            wrong.sink(),
            ReaderState.class,
            EnumState.class,
            WriterState.class,
            String.class);
    env.events.clear();
    reject(() -> env.runner().run(key, changed, store, SETTINGS));
    check(!env.events.contains("reader-create"));
    env.transactional = false;
    reject(() -> env.runner().run(key, env.pipeline(), store, SETTINGS));
    env.transactional = true;
    // Nontransactional output duplicates after an unpublished failure, as explicitly documented.
    var nonTx = new Environment();
    nonTx.transactional = false;
    var nonRepo = new Repository(nonTx.events);
    nonRepo.fail = "publish-before";
    reject(() -> nonTx.runner().run(key, nonTx.pipeline(), nonRepo, SETTINGS));
    nonRepo.fail = "";
    check(nonTx.runner().run(key, nonTx.pipeline(), nonRepo, SETTINGS) == 3);
    check(nonTx.output.equals(List.of(0L, 0L, 1L, 2L)));
    // Idle pipelines still checkpoint and abort a pre-publication snapshot failure.
    var idle = new Environment();
    idle.idle = true;
    idle.fail = "reader-snapshot:1";
    var idleRepo = new Repository(idle.events);
    reject(() -> idle.runner().run(key, idle.pipeline(), idleRepo, SETTINGS));
    check(idle.events.contains("flush:false") && idle.output.isEmpty());
    var cancel = new Environment();
    cancel.idle = true;
    cancel.ticking = false;
    var cancelRepo = new Repository(cancel.events);
    var cancelled = new AtomicBoolean();
    var problem = new AtomicReference<Throwable>();
    var thread =
        new Thread(
            () -> {
              try {
                cancel.runner().run(key, cancel.pipeline(), cancelRepo, SETTINGS);
                problem.set(new AssertionError("Unexpected completion"));
              } catch (InterruptedException expected) {
                cancelled.set(Thread.currentThread().isInterrupted());
              } catch (Throwable t) {
                problem.set(t);
              }
            });
    thread.start();
    check(cancel.polled.await(3, TimeUnit.SECONDS));
    thread.interrupt();
    thread.join(3000);
    check(!thread.isAlive() && problem.get() == null && cancelled.get());
    check(
        cancel.events.contains("reader-close")
            && cancel.events.contains("writer-close")
            && cancel.events.contains("committer-close"));
    reject(() -> new StreamingRunner.Settings(0, 1, 1));
    reject(() -> new StreamingRunner.Settings(1, 0, 1));
    reject(() -> new StreamingRunner.Settings(1, 1, 0));
    System.out.println(
        "PASS streaming/checkpoint contracts: "
            + checks
            + " assertions; serialized test repository, NOT external delivery proof");
  }
}
