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

import java.sql.*;
import java.util.*;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.*;

/** Adapter tests with JDBC proxies. Not official MySQL driver/Hikari/service evidence. */
final class MysqlContracts {
  static Map<String, Object> options(boolean source) {
    var result = source ? JdbcContracts.sourceOptions() : JdbcContracts.options();
    result.put("url", "jdbc:mysql://localhost:3306/public");
    return result;
  }

  static JdbcConfig config(boolean source) {
    return new JdbcConfig(
        options(source), source ? null : JdbcContracts.table(), JdbcDialect.MYSQL);
  }

  static JdbcContracts.Database database() {
    var db = new JdbcContracts.Database();
    db.dialect = JdbcDialect.MYSQL;
    return db;
  }

  public static void main(String[] args) throws Exception {
    JdbcContracts.checks = 0;
    var cfg = config(true);
    JdbcContracts.check(
        cfg.relation().equals("`public`.`events`")
            && cfg.columnSql().equals("`b`,`i`,`l`,`d`,`s`"));
    JdbcContracts.check(!cfg.toString().contains("TOP_SECRET"));
    JdbcContracts.check(
        new MysqlSourcePlugin()
            .createSource(options(true))
            .producedTable()
            .equals(JdbcContracts.table()));
    JdbcContracts.check(
        new MysqlSinkPlugin()
            .createSink(options(false), JdbcContracts.table(), null)
            .createCommitter()
            .isEmpty());
    JdbcContracts.reject(() -> new JdbcConfig(options(true), null));
    JdbcContracts.reject(() -> new MysqlSourcePlugin().createSource(JdbcContracts.sourceOptions()));
    for (String bad :
        List.of(
            "jdbc:mysql:loadbalance://localhost/public",
            "jdbc:mysql://u:TOP_SECRET@localhost/public",
            "jdbc:mysql://localhost/public?allowMultiQueries=true",
            "jdbc:mysql://localhost/other",
            "jdbc:mysql://localhost,other/public",
            "jdbc:mysql://localhost/public#secret",
            "jdbc:mariadb://localhost/public")) {
      var options = options(true);
      options.put("url", bad);
      JdbcContracts.check(
          !JdbcContracts.reject(() -> new MysqlSourcePlugin().createSource(options))
              .toString()
              .contains("TOP_SECRET"));
    }
    for (String key : List.of("schema", "table")) {
      var options = options(true);
      options.put(key, "x` OR 1=1");
      JdbcContracts.reject(() -> new MysqlSourcePlugin().createSource(options));
    }
    for (String mode : List.of("verify-full", "require", "disable")) {
      var options = options(true);
      options.put("ssl-mode", mode);
      var config = new JdbcConfig(options, null, JdbcDialect.MYSQL);
      var properties = config.dialect.properties(config);
      JdbcContracts.check(
          properties
              .get("sslMode")
              .equals(
                  mode.equals("verify-full")
                      ? "VERIFY_IDENTITY"
                      : mode.equals("require") ? "REQUIRED" : "DISABLED"));
      JdbcContracts.check(
          properties.get("connectTimeout").equals("5000")
              && properties.get("socketTimeout").equals("35000"));
      for (String flag :
          List.of(
              "allowMultiQueries",
              "autoReconnect",
              "autoReconnectForPools",
              "allowPublicKeyRetrieval",
              "allowLoadLocalInfile",
              "rewriteBatchedStatements",
              "continueBatchOnError",
              "tinyInt1isBit",
              "profileSQL",
              "dumpQueriesOnException")) JdbcContracts.check(properties.get(flag).equals("false"));
      JdbcContracts.check(
          properties.get("useServerPrepStmts").equals("true")
              && properties.get("useCursorFetch").equals("true"));
      JdbcContracts.check(
          properties.get("paranoid").equals("true")
              && properties.get("logger").equals("com.mysql.cj.log.NullLogger"));
    }
    var defaultOptions = options(true);
    defaultOptions.remove("ssl-mode");
    JdbcContracts.check(
        new JdbcConfig(defaultOptions, null, JdbcDialect.MYSQL).sslMode.equals("verify-full"));
    var pg = new JdbcConfig(JdbcContracts.sourceOptions(), null);
    JdbcContracts.check(
        pg.relation().equals("\"public\".\"events\"")
            && pg.dialect.properties(pg).get("socketTimeout").equals("35"));
    for (boolean signed : List.of(true, false)) {
      var db = database();
      db.signed = signed;
      if (signed) JdbcTypes.validateMetadata(db.proxy(ResultSetMetaData.class), cfg);
      else
        JdbcContracts.reject(
            () -> JdbcTypes.validateMetadata(db.proxy(ResultSetMetaData.class), cfg));
    }
    var width = database();
    width.precision = 8;
    JdbcContracts.reject(
        () -> JdbcTypes.validateMetadata(width.proxy(ResultSetMetaData.class), cfg));
    for (int type : List.of(Types.TINYINT, Types.SMALLINT, Types.DECIMAL, Types.VARCHAR)) {
      var db = database();
      db.overrideType = type;
      JdbcContracts.reject(
          () -> JdbcTypes.validateMetadata(db.proxy(ResultSetMetaData.class), cfg));
    }
    var table = database();
    cfg.dialect.validateTable(table.proxy(Connection.class), cfg);
    JdbcContracts.check(table.events.contains("close"));
    for (String engine : List.of("MyISAM", "MEMORY", "")) {
      var db = database();
      db.engine = engine;
      JdbcContracts.reject(() -> cfg.dialect.validateTable(db.proxy(Connection.class), cfg));
      JdbcContracts.check(db.events.contains("close"));
    }
    var sourceDb = database();
    var source = new JdbcSource(cfg, sourceDb::session);
    var rows = new ArrayList<SeaTunnelRow>();
    try (var reader = source.createReader(0, Optional.empty())) {
      reader.start();
      JdbcContracts.enumerate(source, reader);
      while (reader.pollNext(rows::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
    }
    JdbcContracts.check(
        rows.size() == 2
            && rows.get(1).getField(4) == null
            && sourceDb.events.contains("pool-close"));
    var partitions = options(true);
    partitions.putAll(
        Map.of("partition-column", "i", "lower-bound", 0, "upper-bound", 4, "partitions", 2));
    var partitionCfg = new JdbcConfig(partitions, null, JdbcDialect.MYSQL);
    var partitionDb = database();
    var partitionSource = new JdbcSource(partitionCfg, partitionDb::session);
    try (var reader = partitionSource.createReader(0, Optional.empty())) {
      reader.start();
      JdbcContracts.enumerate(partitionSource, reader);
      while (reader.pollNext(row -> {}) != SourceReader.ReadStatus.END_OF_INPUT) {}
    }
    JdbcContracts.check(
        partitionDb.bound.contains("setLong:1:0") && partitionDb.bound.contains("setLong:2:4"));
    var sinkOptions = options(false);
    sinkOptions.put("batch-size", 2);
    var sinkCfg = new JdbcConfig(sinkOptions, JdbcContracts.table(), JdbcDialect.MYSQL);
    var sinkDb = database();
    var sink = new JdbcSink(sinkCfg, sinkDb::session);
    try (var writer = sink.createWriter(0, Optional.empty())) {
      writer.write(rows.get(0));
      writer.write(rows.get(1));
      writer.flush(true);
    }
    JdbcContracts.check(
        Collections.frequency(sinkDb.events, "commit") == 1
            && sinkDb.events.contains("rollback")
            && sinkDb.events.contains("pool-close"));
    for (RowKind kind : RowKind.values())
      if (kind != RowKind.INSERT) {
        var db = database();
        var writer = new JdbcSink(sinkCfg, db::session).createWriter(0, Optional.empty());
        JdbcContracts.reject(() -> writer.write(rows.get(0).withKind(kind)));
        JdbcContracts.check(db.events.contains("rollback") && !db.events.contains("commit"));
      }
    var fail = database();
    var failedWriter = new JdbcSink(sinkCfg, fail::session).createWriter(0, Optional.empty());
    fail.failure = "executeBatch";
    var failure =
        JdbcContracts.reject(
            () -> {
              failedWriter.write(rows.get(0));
              failedWriter.write(rows.get(1));
            });
    JdbcContracts.check(
        !failure.toString().contains("TOP_SECRET")
            && fail.events.contains("rollback")
            && fail.events.contains("pool-close"));
    System.out.println(
        "PASS MySQL dialect/proxy contracts: "
            + JdbcContracts.checks
            + " assertions (not MySQL service/driver acceptance)");
  }
}
