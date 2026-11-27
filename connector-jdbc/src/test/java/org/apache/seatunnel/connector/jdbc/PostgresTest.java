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
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

/**
 * Real PostgreSQL gate, explicitly skipped without a disposable database. Never substitutes HSQLDB.
 */
@EnabledIfEnvironmentVariable(named = "PINOT_PG_URL", matches = ".+")
class PostgresTest {
  @Test
  void roundTripPartitionsRollbackAndTypedNulls() throws Exception {
    String url = System.getenv("PINOT_PG_URL"),
        user = System.getenv("PINOT_PG_USER"),
        password = System.getenv("PINOT_PG_PASSWORD");
    String schema = "test_jdbc_" + UUID.randomUUID().toString().replace("-", "");
    var properties = new Properties();
    properties.setProperty("user", user);
    properties.setProperty("password", password);
    properties.setProperty(
        "sslmode", System.getenv().getOrDefault("PINOT_PG_SSL_MODE", "verify-full"));
    properties.setProperty("connectTimeout", "5");
    properties.setProperty("socketTimeout", "35");
    try (Connection connection = DriverManager.getConnection(url, properties);
        Statement ddl = connection.createStatement()) {
      ddl.execute("CREATE SCHEMA \"" + schema + "\"");
      try {
        ddl.execute(
            "CREATE TABLE \""
                + schema
                + "\".events (b BOOLEAN NOT NULL, i INTEGER PRIMARY KEY, l BIGINT NOT NULL, d"
                + " DOUBLE PRECISION NOT NULL, s TEXT)");
        var options = JdbcContracts.options();
        options.put("url", url);
        options.put("user", user);
        options.put("password", password);
        options.put("schema", schema);
        options.put("ssl-mode", properties.getProperty("sslmode"));
        options.put("batch-size", 2);
        var sink = new JdbcSinkPlugin().createSink(options, JdbcContracts.table(), null);
        try (var writer = sink.createWriter(0, Optional.empty())) {
          writer.write(new SeaTunnelRow(true, 1, 10L, 1.5, "one"));
          writer.write(new SeaTunnelRow(false, 2, 20L, 2.5, null));
          // This batch must roll back entirely; the first two committed rows remain.
          writer.write(new SeaTunnelRow(true, 3, 30L, 3.5, "rolled-back"));
          JdbcContracts.reject(
              () -> writer.write(new SeaTunnelRow(false, 1, 40L, 4.5, "duplicate")));
          JdbcContracts.reject(() -> writer.flush(true));
        }
        try (var count = ddl.executeQuery("SELECT count(*) FROM \"" + schema + "\".events")) {
          count.next();
          JdbcContracts.check(count.getInt(1) == 2);
        }
        var read = JdbcContracts.sourceOptions();
        read.put("url", url);
        read.put("user", user);
        read.put("password", password);
        read.put("schema", schema);
        read.put("ssl-mode", properties.getProperty("sslmode"));
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
        var source = new JdbcSourcePlugin().createSource(read);
        var rows = new ArrayList<SeaTunnelRow>();
        try (var reader = source.createReader(0, Optional.empty())) {
          reader.start();
          JdbcContracts.enumerate(source, reader);
          while (reader.pollNext(rows::add) != SourceReader.ReadStatus.END_OF_INPUT) {}
        }
        JdbcContracts.check(rows.size() == 2);
        JdbcContracts.check(
            new HashSet<>(rows)
                .equals(
                    Set.of(
                        new SeaTunnelRow(true, 1, 10L, 1.5, "one"),
                        new SeaTunnelRow(false, 2, 20L, 2.5, null))));
        // Closing without a flush must not publish the pending batch.
        try (var writer = sink.createWriter(0, Optional.empty())) {
          writer.write(new SeaTunnelRow(true, 4, 40L, 4.5, "not-flushed"));
        }
        try (var count = ddl.executeQuery("SELECT count(*) FROM \"" + schema + "\".events")) {
          count.next();
          JdbcContracts.check(count.getInt(1) == 2);
        }
      } finally {
        ddl.execute("DROP SCHEMA \"" + schema + "\" CASCADE");
      }
    }
  }
}
