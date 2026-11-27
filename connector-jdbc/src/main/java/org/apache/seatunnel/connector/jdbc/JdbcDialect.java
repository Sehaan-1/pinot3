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

/** Vendor adaptation only: JDBC and the selected official driver own protocol/codecs. */
enum JdbcDialect {
  POSTGRES("jdbc:postgresql://", "org.postgresql.Driver", '\"'),
  MYSQL("jdbc:mysql://", "com.mysql.cj.jdbc.Driver", '`');
  final String prefix, driver;
  private final char delimiter;

  JdbcDialect(String prefix, String driver, char delimiter) {
    this.prefix = prefix;
    this.driver = driver;
    this.delimiter = delimiter;
  }

  String quote(String value) {
    return delimiter + JdbcConfig.identifier(value) + delimiter;
  }

  Map<String, String> properties(JdbcConfig config) {
    if (this == POSTGRES)
      return Map.of(
          "sslmode",
          config.sslMode,
          "connectTimeout",
          "5",
          "socketTimeout",
          Integer.toString(config.timeout + 5),
          "tcpKeepAlive",
          "true",
          "logServerErrorDetail",
          "false");
    var properties = new HashMap<String, String>();
    properties.put(
        "sslMode",
        switch (config.sslMode) {
          case "verify-full" -> "VERIFY_IDENTITY";
          case "require" -> "REQUIRED";
          default -> "DISABLED";
        });
    properties.put("connectTimeout", "5000");
    properties.put("socketTimeout", Integer.toString((config.timeout + 5) * 1000));
    properties.put("useServerPrepStmts", "true");
    properties.put("useCursorFetch", "true");
    properties.put("tinyInt1isBit", "false");
    properties.put("allowMultiQueries", "false");
    properties.put("autoReconnect", "false");
    properties.put("autoReconnectForPools", "false");
    properties.put("allowPublicKeyRetrieval", "false");
    properties.put("allowLoadLocalInfile", "false");
    properties.put("rewriteBatchedStatements", "false");
    properties.put("continueBatchOnError", "false");
    properties.put("paranoid", "true");
    properties.put("dumpQueriesOnException", "false");
    properties.put("profileSQL", "false");
    properties.put("logger", "com.mysql.cj.log.NullLogger");
    return Map.copyOf(properties);
  }

  /** Reject nontransactional tables/views before user IO; concurrent DDL remains unsupported. */
  void validateTable(Connection connection, JdbcConfig config) throws SQLException {
    if (this != MYSQL) return;
    try (var statement =
        connection.prepareStatement(
            "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?")) {
      statement.setQueryTimeout(config.timeout);
      statement.setString(1, config.schema);
      statement.setString(2, config.table);
      try (var result = statement.executeQuery()) {
        if (!result.next() || !"InnoDB".equalsIgnoreCase(result.getString(1)) || result.next())
          throw new SQLException("MySQL adapter requires an existing InnoDB base table");
      }
    }
  }
}
