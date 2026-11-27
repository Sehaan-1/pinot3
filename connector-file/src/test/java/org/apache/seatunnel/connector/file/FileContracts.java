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

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.api.table.CatalogTable.*;
import org.apache.seatunnel.core.LocalRunner;

/** Real temporary-directory operations. Optional args select formats for supplemental runs. */
final class FileContracts {
  static int checks;

  static void check(boolean condition) {
    checks++;
    if (!condition) throw new AssertionError("check " + checks);
  }

  interface Work {
    void run() throws Exception;
  }

  static Exception reject(Work work) throws Exception {
    checks++;
    try {
      work.run();
    } catch (Exception e) {
      return e;
    }
    throw new AssertionError("Expected rejection " + checks);
  }

  static CatalogTable table(String format) {
    return format.equals("text")
        ? new CatalogTable("file", List.of(new Column("line", DataType.STRING, false)))
        : new CatalogTable(
            "file",
            List.of(
                new Column("flag", DataType.BOOLEAN, false),
                new Column("i", DataType.INT, false),
                new Column("l", DataType.LONG, false),
                new Column("f", DataType.FLOAT, false),
                new Column("d", DataType.DOUBLE, false),
                new Column("s", DataType.STRING, true),
                new Column("b", DataType.BYTES, true)));
  }

  static Map<String, Object> options(Path root, String format, String path, boolean source) {
    var m = new HashMap<String, Object>();
    m.put("root", root.toString());
    m.put("format", format);
    m.put(source ? "paths" : "path", source ? List.of(path) : path);
    if (source)
      m.put(
          "schema",
          table(format).columns().stream()
              .map(c -> Map.of("name", c.name(), "type", c.type().name(), "nullable", c.nullable()))
              .toList());
    return m;
  }

  static SeaTunnelRow row(String format, int i) {
    return format.equals("text")
        ? new SeaTunnelRow(i == 0 ? "" : "value-\u03bb-" + i)
        : new SeaTunnelRow(
            true,
            i,
            Long.MAX_VALUE,
            1.25f,
            2.5d,
            i == 1 ? null : i == 0 ? "" : "quote\",comma,\nnew line \u03bb",
            i == 1 ? null : new byte[] {0, 1, (byte) 255});
  }

  static FileSource source(Path root, String format, String path) {
    return new FileSourcePlugin().createSource(options(root, format, path, true));
  }

  static FileSink sink(Path root, String format, String path) {
    return new FileSinkPlugin().createSink(options(root, format, path, false), table(format), null);
  }

  static void assign(FileSource source, SourceReader<FileSource.Split, FileSource.State> reader)
      throws Exception {
    try (var enumerator =
        source.createEnumerator(
            new SplitEnumerator.Context<FileSource.Split>() {
              public int currentParallelism() {
                return 1;
              }

              public void assignSplits(int id, List<FileSource.Split> splits) throws Exception {
                reader.addSplits(splits);
              }

              public void signalNoMoreSplits(int id) throws Exception {
                reader.notifyNoMoreSplits();
              }
            },
            Optional.empty())) {
      enumerator.start();
      enumerator.registerReader(0);
      enumerator.handleSplitRequest(0);
      reject(() -> enumerator.snapshotState(1));
    }
  }

  static List<SeaTunnelRow> read(FileSource source) throws Exception {
    var result = new ArrayList<SeaTunnelRow>();
    try (var reader = source.createReader(0, Optional.empty())) {
      reader.start();
      assign(source, reader);
      while (reader.pollNext(result::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
    }
    return result;
  }

  static void noStage(Path root) throws Exception {
    try (var entries = Files.list(root)) {
      check(entries.noneMatch(p -> p.getFileName().toString().endsWith(".stage")));
    }
  }

  static void same(SeaTunnelRow actual, SeaTunnelRow expected) {
    check(actual.getArity() == expected.getArity());
    for (int i = 0; i < actual.getArity(); i++) {
      Object a = actual.getField(i), e = expected.getField(i);
      check(
          e instanceof byte[] bytes
              ? a instanceof byte[] value && Arrays.equals(value, bytes)
              : Objects.equals(a, e));
    }
  }

  public static void main(String[] args) throws Exception {
    checks = 0;
    var formats =
        args.length == 0 ? List.of("csv", "json", "text", "parquet", "orc") : List.of(args);
    Path root = Files.createTempDirectory("file-contracts-");
    try {
      check(
          java.util.ServiceLoader.load(org.apache.seatunnel.plugin.SourcePlugin.class).stream()
              .anyMatch(p -> p.type() == FileSourcePlugin.class));
      check(
          java.util.ServiceLoader.load(org.apache.seatunnel.plugin.SinkPlugin.class).stream()
              .anyMatch(p -> p.type() == FileSinkPlugin.class));
      // Pure factory validation: a missing absolute root need not exist until runtime.
      var missing = root.resolve("missing");
      sink(missing, "json", "output.json");
      source(missing, "json", "input.json");
      check(!Files.exists(missing));
      for (String f : formats) {
        var sink = sink(root, f, "input." + f);
        check(sink.createCommitter().isEmpty());
        var writer = sink.createWriter(0, Optional.empty());
        check(!Files.exists(root.resolve("input." + f)));
        for (int i = 0; i < 3; i++) writer.write(row(f, i));
        writer.flush(false);
        check(!Files.exists(root.resolve("input." + f)));
        reject(() -> writer.snapshotState(1));
        reject(() -> writer.prepareCommit(1));
        writer.flush(true);
        writer.flush(true);
        check(Files.isRegularFile(root.resolve("input." + f)));
        reject(() -> writer.write(row(f, 0)));
        writer.close();
        writer.close();
        noStage(root);
        var source = source(root, f, "input." + f);
        check(source.boundedness() == Boundedness.BOUNDED);
        var result = read(source);
        check(result.size() == 3);
        for (int i = 0; i < 3; i++) same(result.get(i), row(f, i));
        // Batch boundary and detached binary payload through each real format implementation.
        try (var large = sink(root, f, "batch." + f).createWriter(0, Optional.empty())) {
          for (int i = 0; i < 300; i++) large.write(row(f, i));
          large.flush(true);
        }
        var batchRows = read(source(root, f, "batch." + f));
        check(batchRows.size() == 300);
        same(batchRows.get(299), row(f, 299));
        if (!f.equals("text")) {
          var wrong = options(root, f, "input." + f, true);
          var columns =
              new ArrayList<>(
                  table(f).columns().stream()
                      .map(
                          c ->
                              Map.<String, Object>of(
                                  "name",
                                  c.name(),
                                  "type",
                                  c.type().name(),
                                  "nullable",
                                  c.nullable()))
                      .toList());
          columns.set(0, Map.of("name", "flag", "type", "INT", "nullable", false));
          wrong.put("schema", columns);
          reject(() -> read(new FileSourcePlugin().createSource(wrong)));
        }
        var bytes = Files.readAllBytes(root.resolve("input." + f));
        reject(() -> sink.createWriter(0, Optional.empty()));
        check(Arrays.equals(bytes, Files.readAllBytes(root.resolve("input." + f))));
        var abandon = sink(root, f, "abandon." + f).createWriter(0, Optional.empty());
        abandon.write(row(f, 0));
        abandon.close();
        check(!Files.exists(root.resolve("abandon." + f)));
        noStage(root);
        try (var empty = sink(root, f, "empty." + f).createWriter(0, Optional.empty())) {
          empty.flush(true);
        }
        check(read(source(root, f, "empty." + f)).isEmpty());
        reject(() -> source.createReader(0, Optional.of(new FileSource.State())));
        reject(() -> sink.createWriter(0, Optional.of(new FileSink.State())));
        reject(() -> source.createReader(1, Optional.empty()));
        reject(() -> sink.createWriter(1, Optional.empty()));
        // Existing target introduced during write: no replacement, stage cleaned on failure.
        var race = sink(root, f, "race." + f).createWriter(0, Optional.empty());
        race.write(row(f, 0));
        Files.writeString(root.resolve("race." + f), "existing");
        reject(() -> race.flush(true));
        check(Files.readString(root.resolve("race." + f)).equals("existing"));
        noStage(root);
        for (RowKind kind : RowKind.values())
          if (kind != RowKind.INSERT) {
            var w = sink(root, f, "kind." + f).createWriter(0, Optional.empty());
            reject(() -> w.write(row(f, 0).withKind(kind)));
            w.close();
            check(!Files.exists(root.resolve("kind." + f)));
            noStage(root);
          }
        var invalid = sink(root, f, "invalid." + f).createWriter(0, Optional.empty());
        reject(() -> invalid.write(new SeaTunnelRow(1)));
        check(!Files.exists(root.resolve("invalid." + f)));
        noStage(root);
        var limited = options(root, f, "limit." + f, false);
        limited.put("max-row-bytes", 1);
        var limitSink = new FileSinkPlugin().createSink(limited, table(f), null);
        var limitWriter = limitSink.createWriter(0, Optional.empty());
        reject(() -> limitWriter.write(row(f, 2)));
        noStage(root);
        var fileLimit = options(root, f, "file-limit." + f, false);
        fileLimit.put("max-file-bytes", 1);
        var fileWriter =
            new FileSinkPlugin()
                .createSink(fileLimit, table(f), null)
                .createWriter(0, Optional.empty());
        reject(
            () -> {
              fileWriter.write(row(f, 2));
              fileWriter.flush(true);
            });
        check(!Files.exists(root.resolve("file-limit." + f)));
        noStage(root);
        // Real LocalRunner bounded source->staged sink, not direct codec-only proof.
        check(new LocalRunner().execute(source, sink(root, f, "copy." + f)) == 3);
        var copied = read(source(root, f, "copy." + f));
        for (int i = 0; i < 3; i++) same(copied.get(i), row(f, i));
        var failureReader = source.createReader(0, Optional.empty());
        failureReader.start();
        assign(source, failureReader);
        reject(
            () ->
                failureReader.pollNext(
                    r -> {
                      throw new IOException("PRIVATE_FILE_DATA");
                    }));
        reject(() -> failureReader.pollNext(r -> {}));
        failureReader.close();
        var interrupted = sink(root, f, "interrupted." + f).createWriter(0, Optional.empty());
        Thread.currentThread().interrupt();
        try {
          check(reject(() -> interrupted.write(row(f, 0))) instanceof InterruptedException);
          check(Thread.currentThread().isInterrupted());
        } finally {
          Thread.interrupted();
        }
        noStage(root);
      }
      for (String path :
          List.of("../outside", "/absolute", "a/../b", "a/./b", "file:///tmp/a", "a\\b", "a//b")) {
        reject(() -> sink(root, "json", path));
        reject(() -> source(root, "json", path));
      }
      for (Object value : List.of(0, -1, 1.5, "1")) {
        var bad = options(root, "json", "x", false);
        bad.put("max-file-bytes", value);
        reject(() -> new FileSinkPlugin().createSink(bad, table("json"), null));
      }
      var unknown = options(root, "json", "x", true);
      unknown.put("unknown", "PRIVATE_FILE_DATA");
      reject(() -> new FileSourcePlugin().createSource(unknown));
      var duplicate = options(root, "json", "x", true);
      duplicate.put("paths", List.of("x", "x"));
      reject(() -> new FileSourcePlugin().createSource(duplicate));
      var foreign = source(root, "json", "x");
      var r = foreign.createReader(0, Optional.empty());
      reject(() -> r.pollNext(x -> {}));
      r.start();
      reject(r::start);
      reject(() -> r.addSplits(List.of(new FileSource.Split("other", 0, "x", "x"))));
      reject(r::notifyNoMoreSplits);
      reject(() -> r.snapshotState(1));
      r.close();
      r.close();
      reject(() -> r.pollNext(x -> {}));
      // Symlink root, ancestor and leaf all rejected at runtime; relative traversal rejected
      // earlier.
      Files.createDirectory(root.resolve("directory"));
      Files.createSymbolicLink(root.resolve("link"), root.resolve("directory"));
      reject(() -> sink(root, "json", "link/output").createWriter(0, Optional.empty()));
      Files.createSymbolicLink(root.resolve("leaf"), root.resolve("directory"));
      reject(() -> sink(root, "json", "leaf").createWriter(0, Optional.empty()));
      reject(() -> sink(root.resolve("link"), "json", "output").createWriter(0, Optional.empty()));
      Files.writeString(root.resolve("mutating.json"), "[true,1,2,1.0,2.0,\"s\",null]\n");
      var s = source(root, "json", "mutating.json");
      var reader = s.createReader(0, Optional.empty());
      reader.start();
      assign(s, reader);
      Files.writeString(root.resolve("mutating.json"), "changed-size");
      reject(() -> reader.pollNext(x -> {}));
      reader.close();
      if (formats.contains("json"))
        for (String malformed :
            List.of(
                "{}",
                "[true,1,2,1,2,null,null] {}",
                "[true,2147483648,2,1,2,null,null]",
                "[true,1,2,1,2,null,4]",
                "[null,1,2,1,2,null,null]",
                "[true,1,2,1,2,null]",
                "")) {
          Files.writeString(root.resolve("bad.json"), malformed + "\n");
          reject(() -> read(source(root, "json", "bad.json")));
        }
      if (formats.contains("json")) {
        Files.write(root.resolve("utf8.json"), new byte[] {(byte) 0xff, 10});
        reject(() -> read(source(root, "json", "utf8.json")));
      }
      if (formats.contains("csv")) {
        Files.writeString(root.resolve("broken.csv"), "\"unterminated");
        reject(() -> read(source(root, "csv", "broken.csv")));
      }
      if (formats.contains("csv")) {
        var w = sink(root, "csv", "reserved.csv").createWriter(0, Optional.empty());
        reject(() -> w.write(new SeaTunnelRow(true, 0, 1L, 1f, 1d, "\\N", null)));
        noStage(root);
      }
      if (formats.contains("text")) {
        var w = sink(root, "text", "multiline.txt").createWriter(0, Optional.empty());
        reject(() -> w.write(new SeaTunnelRow("a\nb")));
        noStage(root);
      }
      System.out.println(
          "PASS local file contracts: "
              + checks
              + " assertions; real formats "
              + formats
              + "; real NIO hard-link publication + LocalRunner");
    } finally {
      try (var paths = Files.walk(root)) {
        for (Path p : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(p);
      }
    }
  }
}
