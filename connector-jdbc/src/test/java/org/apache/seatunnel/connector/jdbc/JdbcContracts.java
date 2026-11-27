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
package org.apache.seatunnel.connector.jdbc;

import java.lang.reflect.*;
import java.sql.*;
import java.util.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.api.table.CatalogTable.*;

/** Offline executable public/lifecycle/failure contracts; JDBC proxies are not PostgreSQL E2E. */
final class JdbcContracts {
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
    throw new AssertionError("Expected rejection " + checks);
  }

  static Map<String, Object> options() {
    return new HashMap<>(
        Map.of(
            "url",
            "jdbc:postgresql://localhost:5432/test",
            "user",
            "test",
            "password",
            "TOP_SECRET",
            "schema",
            "public",
            "table",
            "events",
            "ssl-mode",
            "disable"));
  }

  static List<Column> columns() {
    return List.of(
        new Column("b", DataType.BOOLEAN, false),
        new Column("i", DataType.INT, false),
        new Column("l", DataType.LONG, false),
        new Column("d", DataType.DOUBLE, false),
        new Column("s", DataType.STRING, true));
  }

  static Map<String, Object> sourceOptions() {
    var options = options();
    options.put(
        "columns",
        columns().stream()
            .map(c -> Map.of("name", c.name(), "type", c.type().name(), "nullable", c.nullable()))
            .toList());
    return options;
  }

  static CatalogTable table() {
    return new CatalogTable("events", columns());
  }

  static final class Database implements InvocationHandler {
    final List<String> events = new ArrayList<>();
    final List<Object> bound = new ArrayList<>();
    String failure = "";
    int batches, cursor = -1;
    boolean nullCell;
    Object[][] rows = {{true, 1, 10L, 1.5, "one"}, {false, 2, 20L, 2.5, null}};

    @SuppressWarnings("unchecked")
    <T> T proxy(Class<T> type) {
      return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, this);
    }

    JdbcSession session(JdbcConfig config, boolean readOnly) {
      events.add("session");
      return new JdbcSession(proxy(Connection.class), () -> events.add("pool-close"));
    }

    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String name = method.getName();
      events.add(name);
      if (failure.equals(name)) throw new SQLException("TOP_SECRET driver data");
      switch (name) {
        case "prepareStatement":
          check(((String) args[0]).contains("\"public\".\"events\""));
          return proxy(PreparedStatement.class);
        case "setBoolean", "setInt", "setLong", "setDouble", "setString", "setNull":
          bound.add(name + ":" + args[0] + ":" + args[1]);
          return null;
        case "addBatch":
          batches++;
          return null;
        case "executeBatch":
          int[] result = new int[batches];
          Arrays.fill(result, 1);
          return result;
        case "clearBatch":
          batches = 0;
          return null;
        case "executeQuery":
          cursor = -1;
          return proxy(ResultSet.class);
        case "getMetaData":
          return proxy(ResultSetMetaData.class);
        case "getColumnCount":
          return 5;
        case "getColumnType":
          return JdbcTypes.sqlType(columns().get((int) args[0] - 1).type());
        case "getColumnLabel":
          return columns().get((int) args[0] - 1).name();
        case "next":
          return ++cursor < rows.length;
        case "getBoolean", "getInt", "getLong", "getDouble", "getString":
          Object value = rows[cursor][(int) args[0] - 1];
          nullCell = value == null;
          if (value != null) return value;
          if (name.equals("getString")) return null;
          if (name.equals("getBoolean")) return false;
          if (name.equals("getLong")) return 0L;
          if (name.equals("getDouble")) return 0.0;
          return 0;
        case "wasNull":
          return nullCell;
        default:
          if (method.getReturnType() == boolean.class) return false;
          if (method.getReturnType() == int.class) return 0;
          if (method.getReturnType() == long.class) return 0L;
          return null;
      }
    }
  }

  static void enumerate(JdbcSource source, SourceReader<JdbcSource.Split, JdbcSource.State> reader)
      throws Exception {
    try (var enumerator =
        source.createEnumerator(
            new SplitEnumerator.Context<JdbcSource.Split>() {
              public int currentParallelism() {
                return 1;
              }

              public void assignSplits(int id, List<JdbcSource.Split> splits) throws Exception {
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
      reject(() -> enumerator.snapshotState(0));
      reject(() -> enumerator.addSplitsBack(List.of(), 0));
    }
  }

  public static void main(String[] args) throws Exception {
    checks = 0;
    var cfg = new JdbcConfig(sourceOptions(), null);
    check(!cfg.toString().contains("TOP_SECRET"));
    check(
        new JdbcSourcePlugin().createSource(sourceOptions()).boundedness() == Boundedness.BOUNDED);
    check(new JdbcSinkPlugin().createSink(options(), table(), null).createCommitter().isEmpty());
    for (String key : List.of("url", "user", "password", "schema", "table", "columns")) {
      var bad = sourceOptions();
      bad.remove(key);
      reject(() -> new JdbcConfig(bad, null));
    }
    for (String bad :
        List.of(
            "jdbc:mysql://localhost/test",
            "jdbc:postgresql://u:p@localhost/test",
            "jdbc:postgresql://localhost/test?password=TOP_SECRET",
            "jdbc:postgresql://localhost/test#x",
            "jdbc:postgresql://localhost/",
            "jdbc:postgresql://localhost:0/test")) {
      var map = sourceOptions();
      map.put("url", bad);
      check(!reject(() -> new JdbcConfig(map, null)).toString().contains("TOP_SECRET"));
    }
    for (String key : List.of("table", "schema")) {
      var map = sourceOptions();
      map.put(key, "x; DROP TABLE x");
      reject(() -> new JdbcConfig(map, null));
    }
    for (String key : List.of("fetch-size", "query-timeout-seconds"))
      for (Object bad : List.of(0, 10001, 1.5, "1")) {
        var map = sourceOptions();
        map.put(key, bad);
        reject(() -> new JdbcConfig(map, null));
      }
    var unknown = sourceOptions();
    unknown.put("query", "SELECT 1");
    reject(() -> new JdbcConfig(unknown, null));
    var partitions = sourceOptions();
    partitions.putAll(
        Map.of(
            "partition-column",
            "l",
            "lower-bound",
            Long.MIN_VALUE,
            "upper-bound",
            Long.MAX_VALUE,
            "partitions",
            32));
    var ranged = new JdbcConfig(partitions, null);
    check(
        ranged.splits.size() == 32
            && ranged.splits.get(0).lower() == Long.MIN_VALUE
            && ranged.splits.get(31).upper() == Long.MAX_VALUE);
    for (int i = 1; i < 32; i++)
      check(ranged.splits.get(i - 1).upper().equals(ranged.splits.get(i).lower()));
    for (String key : List.of("lower-bound", "upper-bound", "partitions", "partition-column")) {
      var bad = new HashMap<>(partitions);
      bad.remove(key);
      reject(() -> new JdbcConfig(bad, null));
    }
    var small = new HashMap<>(partitions);
    small.put("lower-bound", 0);
    small.put("upper-bound", 1);
    reject(() -> new JdbcConfig(small, null));
    var unsupported = new CatalogTable("t", List.of(new Column("x", DataType.BYTES, true)));
    reject(() -> new JdbcConfig(options(), unsupported));
    for (DataType type : DataType.values())
      if (!Set.of(DataType.BOOLEAN, DataType.INT, DataType.LONG, DataType.DOUBLE, DataType.STRING)
          .contains(type)) reject(() -> JdbcTypes.sqlType(type));
    for (Column column : columns()) {
      if (!column.nullable()) reject(() -> JdbcTypes.validate(null, column));
      reject(() -> JdbcTypes.validate(new Object(), column));
    }
    var sourceDb = new Database();
    var source = new JdbcSource(cfg, sourceDb::session);
    var reader = source.createReader(0, Optional.empty());
    check(sourceDb.events.isEmpty());
    reject(() -> reader.pollNext(row -> {}));
    reader.start();
    reject(reader::start);
    reject(() -> source.createReader(1, Optional.empty()));
    reject(() -> source.createReader(0, Optional.of(new JdbcSource.State())));
    reject(() -> reader.addSplits(List.of(new JdbcSource.Split(2, 1L, 2L))));
    enumerate(source, reader);
    reject(() -> reader.addSplits(cfg.splits));
    reject(() -> reader.snapshotState(0));
    List<SeaTunnelRow> collected = new ArrayList<>();
    while (reader.pollNext(collected::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
    check(
        collected.size() == 2
            && collected.get(1).getField(4) == null
            && collected.get(0).getField(2).equals(10L));
    check(collected.stream().allMatch(r -> r.getRowKind() == RowKind.INSERT));
    reader.close();
    reader.close();
    reject(() -> reader.pollNext(collected::add));
    check(sourceDb.events.contains("rollback") && sourceDb.events.contains("pool-close"));
    for (String fault :
        List.of(
            "prepareStatement",
            "setFetchSize",
            "executeQuery",
            "getMetaData",
            "next",
            "getLong",
            "close")) {
      var db = new Database();
      var s = new JdbcSource(cfg, db::session);
      var r = s.createReader(0, Optional.empty());
      r.start();
      enumerate(s, r);
      db.failure = fault;
      if (fault.equals("close")) reject(r::close);
      else {
        var error = reject(() -> r.pollNext(row -> {}));
        check(!error.toString().contains("TOP_SECRET") && error.getCause() == null);
        reject(() -> r.pollNext(row -> {}));
      }
      check(db.events.contains("pool-close"));
    }
    var outputDb = new Database();
    var outputSource = new JdbcSource(cfg, outputDb::session);
    var outputReader = outputSource.createReader(0, Optional.empty());
    outputReader.start();
    enumerate(outputSource, outputReader);
    reject(
        () ->
            outputReader.pollNext(
                row -> {
                  throw new IllegalStateException("downstream");
                }));
    check(outputDb.events.contains("pool-close"));
    reject(() -> outputReader.pollNext(row -> {}));
    var sinkOptions = options();
    sinkOptions.put("batch-size", 2);
    var sinkCfg = new JdbcConfig(sinkOptions, table());
    var db = new Database();
    var sink = new JdbcSink(sinkCfg, db::session);
    var writer = sink.createWriter(0, Optional.empty());
    writer.write(collected.get(0));
    check(!db.events.contains("commit"));
    writer.write(collected.get(1));
    check(Collections.frequency(db.events, "commit") == 1 && db.bound.contains("setNull:5:12"));
    writer.flush(true);
    check(Collections.frequency(db.events, "commit") == 1);
    writer.close();
    writer.close();
    check(db.events.contains("rollback") && db.events.contains("pool-close"));
    reject(() -> writer.write(collected.get(0)));
    for (String fault : List.of("setLong", "addBatch", "executeBatch", "clearBatch", "commit")) {
      var failureDb = new Database();
      var failed = new JdbcSink(sinkCfg, failureDb::session).createWriter(0, Optional.empty());
      failureDb.failure = fault;
      var error =
          reject(
              () -> {
                failed.write(collected.get(0));
                failed.write(collected.get(1));
              });
      check(!error.toString().contains("TOP_SECRET"));
      check(failureDb.events.contains("rollback") && failureDb.events.contains("pool-close"));
      reject(() -> failed.flush(true));
    }
    for (RowKind kind : RowKind.values())
      if (kind != RowKind.INSERT) {
        var kindDb = new Database();
        var w = new JdbcSink(sinkCfg, kindDb::session).createWriter(0, Optional.empty());
        reject(() -> w.write(collected.get(0).withKind(kind)));
        check(!kindDb.events.contains("commit") && kindDb.events.contains("pool-close"));
      }
    var unflushedDb = new Database();
    var unflushed = new JdbcSink(sinkCfg, unflushedDb::session).createWriter(0, Optional.empty());
    unflushed.write(collected.get(0));
    unflushed.close();
    check(!unflushedDb.events.contains("executeBatch") && !unflushedDb.events.contains("commit"));
    var initDb = new Database();
    initDb.failure = "prepareStatement";
    reject(() -> new JdbcSink(sinkCfg, initDb::session).createWriter(0, Optional.empty()));
    check(initDb.events.contains("pool-close"));
    System.out.println(
        "PASS JDBC proxy/config contracts: " + checks + " assertions (not PostgreSQL E2E)");
  }
}
