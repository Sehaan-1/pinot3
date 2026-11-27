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

import java.net.URI;
import java.sql.*;
import java.util.*;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.api.table.CatalogTable.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Real MySQL/official-driver gate; requires explicit mysql-driver-tests profile and disposable DB.
 */
@EnabledIfEnvironmentVariable(named = "PINOT_MYSQL_URL", matches = ".+")
class MysqlServiceTest {
  @Test
  void innoDbRoundTripPartitionsRollbackMetadataAndQuoting() throws Exception {
    Class.forName("com.mysql.cj.jdbc.Driver");
    String url = System.getenv("PINOT_MYSQL_URL"),
        user = System.getenv("PINOT_MYSQL_USER"),
        password = System.getenv("PINOT_MYSQL_PASSWORD");
    String schema = URI.create(url.substring(5)).getPath().substring(1);
    String table = "test_mysql_" + UUID.randomUUID().toString().replace("-", "");
    var properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    properties.setProperty(
        "sslMode", System.getenv().getOrDefault("PINOT_MYSQL_SSL_MODE", "VERIFY_IDENTITY"));
    properties.setProperty("connectTimeout", "5000");
    properties.setProperty("socketTimeout", "35000");
    properties.setProperty("allowPublicKeyRetrieval", "false");
    properties.setProperty("logger", "com.mysql.cj.log.NullLogger");
    String mode =
        switch (properties.getProperty("sslMode")) {
          case "VERIFY_IDENTITY" -> "verify-full";
          case "REQUIRED" -> "require";
          case "DISABLED" -> "disable";
          default -> throw new IllegalArgumentException("Unsupported test sslMode");
        };
    var options = MysqlContracts.options(false);
    options.put("url", url);
    options.put("user", user);
    options.put("password", password);
    options.put("schema", schema);
    options.put("table", table);
    options.put("ssl-mode", mode);
    options.put("batch-size", 2);
    var read = MysqlContracts.options(true);
    read.putAll(options);
    read.remove("batch-size");
    try (var connection = DriverManager.getConnection(url, properties);
        var ddl = connection.createStatement()) {
      ddl.execute(
          "CREATE TABLE `"
              + table
              + "` (b BIT(1) NOT NULL,i INT PRIMARY KEY,l BIGINT NOT NULL,d DOUBLE NOT NULL,s TEXT)"
              + " ENGINE=InnoDB");
      try {
        var sink = new MysqlSinkPlugin().createSink(options, JdbcContracts.table(), null);
        try (var writer = sink.createWriter(0, Optional.empty())) {
          writer.write(new SeaTunnelRow(true, 1, 10L, 1.5, "one"));
          writer.write(new SeaTunnelRow(false, 2, 20L, 2.5, null));
          writer.write(new SeaTunnelRow(true, 3, 30L, 3.5, "rollback"));
          JdbcContracts.reject(
              () -> writer.write(new SeaTunnelRow(false, 1, 40L, 4.5, "duplicate")));
          JdbcContracts.reject(() -> writer.flush(true));
        }
        try (var count = ddl.executeQuery("SELECT count(*) FROM `" + table + "`")) {
          count.next();
          JdbcContracts.check(count.getInt(1) == 2);
        }
        read.putAll(
            Map.of(
                "partition-column",
                "i",
                "lower-bound",
                1,
                "upper-bound",
                4,
                "partitions",
                3,
                "fetch-size",
                1));
        var rows = scan(read);
        JdbcContracts.check(
            new HashSet<>(rows)
                    .equals(
                        Set.of(
                            new SeaTunnelRow(true, 1, 10L, 1.5, "one"),
                            new SeaTunnelRow(false, 2, 20L, 2.5, null)))
                && rows.size() == 2);
        try (var writer = sink.createWriter(0, Optional.empty())) {
          writer.write(new SeaTunnelRow(true, 4, 40L, 4.5, "pending-close"));
        }
        try (var count = ddl.executeQuery("SELECT count(*) FROM `" + table + "`")) {
          count.next();
          JdbcContracts.check(count.getInt(1) == 2);
        }
      } finally {
        ddl.execute("DROP TABLE `" + table + "`");
      }
      String reserved = table + "_q";
      ddl.execute("CREATE TABLE `" + reserved + "` (`select` BIGINT NOT NULL) ENGINE=InnoDB");
      try {
        var write = new HashMap<>(options);
        write.put("table", reserved);
        write.put("batch-size", 1);
        var quotedSchema =
            new CatalogTable("quoted", List.of(new Column("select", DataType.LONG, false)));
        try (var writer =
            new MysqlSinkPlugin()
                .createSink(write, quotedSchema, null)
                .createWriter(0, Optional.empty())) {
          writer.write(new SeaTunnelRow(99L));
        }
        var quoted = new HashMap<>(write);
        quoted.remove("batch-size");
        quoted.put("columns", List.of(Map.of("name", "select", "type", "LONG", "nullable", false)));
        JdbcContracts.check(scan(quoted).equals(List.of(new SeaTunnelRow(99L))));
      } finally {
        ddl.execute("DROP TABLE `" + reserved + "`");
      }
      // Views have no InnoDB engine and cannot honor this adapter's base-table contract.
      String view = table + "_v";
      ddl.execute("CREATE VIEW `" + view + "` AS SELECT 1 AS i");
      try {
        var invalid = new HashMap<>(options);
        invalid.put("table", view);
        JdbcContracts.reject(
            () ->
                new MysqlSinkPlugin()
                    .createSink(invalid, JdbcContracts.table(), null)
                    .createWriter(0, Optional.empty()));
      } finally {
        ddl.execute("DROP VIEW `" + view + "`");
      }
      String unsigned = table + "_u";
      ddl.execute("CREATE TABLE `" + unsigned + "` (`select` BIGINT UNSIGNED) ENGINE=InnoDB");
      try {
        var invalid = new HashMap<>(options);
        invalid.remove("batch-size");
        invalid.put("table", unsigned);
        invalid.put("columns", List.of(Map.of("name", "select", "type", "LONG", "nullable", true)));
        JdbcContracts.reject(() -> scan(invalid));
      } finally {
        ddl.execute("DROP TABLE `" + unsigned + "`");
      }
    }
  }

  static List<SeaTunnelRow> scan(Map<String, Object> options) throws Exception {
    var source = new MysqlSourcePlugin().createSource(options);
    var rows = new ArrayList<SeaTunnelRow>();
    try (var reader = source.createReader(0, Optional.empty())) {
      reader.start();
      JdbcContracts.enumerate(source, reader);
      while (reader.pollNext(rows::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
    }
    return rows;
  }
}
