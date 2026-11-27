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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;

/** Runtime-only pool/connection owner; no IO during plugin construction. */
final class JdbcSession implements AutoCloseable {
  final Connection connection;
  private final AutoCloseable owner;
  private boolean closed;

  @FunctionalInterface
  interface Factory {
    JdbcSession open(JdbcConfig config, boolean readOnly) throws Exception;
  }

  JdbcSession(Connection connection, AutoCloseable owner) {
    this.connection = connection;
    this.owner = owner;
  }

  static JdbcSession open(JdbcConfig config, boolean readOnly) throws SQLException {
    HikariDataSource pool = null;
    try {
      HikariConfig settings = new HikariConfig();
      settings.setJdbcUrl(config.url);
      settings.setDriverClassName("org.postgresql.Driver");
      settings.setUsername(config.user);
      settings.setPassword(config.password);
      settings.setMaximumPoolSize(1);
      settings.setMinimumIdle(0);
      settings.setConnectionTimeout(10000);
      settings.setValidationTimeout(5000);
      settings.setInitializationFailTimeout(1);
      settings.setAutoCommit(false);
      settings.setReadOnly(readOnly);
      settings.setTransactionIsolation(
          readOnly ? "TRANSACTION_REPEATABLE_READ" : "TRANSACTION_READ_COMMITTED");
      settings.addDataSourceProperty("sslmode", config.sslMode);
      settings.addDataSourceProperty("connectTimeout", "5");
      settings.addDataSourceProperty("socketTimeout", Integer.toString(config.timeout + 5));
      settings.addDataSourceProperty("tcpKeepAlive", "true");
      settings.addDataSourceProperty("logServerErrorDetail", "false");
      pool = new HikariDataSource(settings);
      return new JdbcSession(pool.getConnection(), pool);
    } catch (Exception failure) {
      if (pool != null) {
        try {
          pool.close();
        } catch (Exception ignored) {
          /* Preserve redacted startup failure. */
        }
      }
      throw databaseFailure();
    }
  }

  static SQLException databaseFailure() {
    return new SQLException("PostgreSQL JDBC operation failed (driver details redacted)");
  }

  @Override
  public void close() throws Exception {
    if (closed) return;
    closed = true;
    try {
      try {
        connection.rollback();
      } finally {
        connection.close();
      }
    } finally {
      owner.close();
    }
  }
}
