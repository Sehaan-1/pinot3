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
package org.apache.seatunnel.transform;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Shared thread-confined VALUES-row query lifecycle and typed JDBC bindings. */
final class SqlRowQuery implements AutoCloseable {
  static final int MAX_STRING_CHARS = 1_048_576;
  private final CatalogTable input;
  private final String query;
  private final List<StringParameter> parameters;
  private Connection connection;
  private PreparedStatement statement;
  private boolean opened, closed;

  SqlRowQuery(CatalogTable input, String query) {
    this(input, query, List.of());
  }

  SqlRowQuery(CatalogTable input, String query, List<StringParameter> parameters) {
    this.parameters = List.copyOf(parameters);
    this.input = Objects.requireNonNull(input, "input");
    this.query = Objects.requireNonNull(query, "query");
  }

  public void open() {
    if (opened || closed)
      throw new IllegalStateException("Transform cannot be opened twice or after close");
    try {
      Class.forName("org.hsqldb.jdbc.JDBCDriver");
    } catch (ClassNotFoundException unavailable) {
      throw new IllegalStateException("SQL driver unavailable", unavailable);
    }
    Connection candidate = null;
    try {
      candidate =
          DriverManager.getConnection(
              "jdbc:hsqldb:mem:seatunnel-query-" + UUID.randomUUID() + ";shutdown=true", "SA", "");
      PreparedStatement prepared = candidate.prepareStatement(query);
      try {
        prepared.setMaxRows(1);
        prepared.setQueryTimeout(2);
      } catch (SQLException failure) {
        try {
          prepared.close();
        } catch (SQLException cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
      connection = candidate;
      statement = prepared;
      opened = true;
    } catch (SQLException | RuntimeException failure) {
      if (candidate != null)
        try {
          candidate.close();
        } catch (SQLException cleanup) {
          failure.addSuppressed(cleanup);
        }
      throw new IllegalStateException("SQL row query could not be prepared", failure);
    }
  }

  <T> T evaluate(SeaTunnelRow row, ResultReader<T> reader) {
    if (!opened || closed) throw new IllegalStateException("Transform is not open");
    if (Objects.requireNonNull(row, "row").getArity() != input.columns().size())
      throw new IllegalArgumentException("Input arity mismatch");
    try (SqlCleanup reset = statement::clearParameters) {
      for (int i = 0; i < row.getArity(); i++) bind(i, row.getField(i));
      for (int i = 0; i < parameters.size(); i++) {
        String value = parameters.get(i).value();
        int index = row.getArity() + i + 1;
        if (value == null) statement.setNull(index, Types.VARCHAR);
        else statement.setString(index, value);
      }
      try (ResultSet result = statement.executeQuery()) {
        return reader.read(result);
      }
    } catch (SQLException failure) {
      throw new IllegalStateException("SQL row query evaluation failed", failure);
    }
  }

  private void bind(int position, Object value) throws SQLException {
    int index = position + 1;
    switch (input.columns().get(position).type()) {
      case BOOLEAN -> {
        if (value == null) statement.setNull(index, Types.BOOLEAN);
        else if (value instanceof Boolean typed) statement.setBoolean(index, typed);
        else throw new IllegalArgumentException("Expected Boolean SQL field");
      }
      case INT -> {
        if (value == null) statement.setNull(index, Types.INTEGER);
        else if (value instanceof Integer typed) statement.setInt(index, typed);
        else throw new IllegalArgumentException("Expected Integer SQL field");
      }
      case LONG -> {
        if (value == null) statement.setNull(index, Types.BIGINT);
        else if (value instanceof Long typed) statement.setLong(index, typed);
        else throw new IllegalArgumentException("Expected Long SQL field");
      }
      case STRING -> {
        if (value == null) statement.setNull(index, Types.VARCHAR);
        else if (value instanceof String text && text.length() <= MAX_STRING_CHARS)
          statement.setString(index, text);
        else throw new IllegalArgumentException("Expected bounded String SQL field");
      }
      default -> throw new IllegalArgumentException("Unsupported SQL field type");
    }
  }

  @Override
  public void close() throws SQLException {
    if (closed) return;
    closed = true;
    Connection ownedConnection = connection;
    PreparedStatement ownedStatement = statement;
    connection = null;
    statement = null;
    try (Connection ignoredConnection = ownedConnection;
        PreparedStatement ignoredStatement = ownedStatement) {
      // JDK closes the statement before the private database connection, suppressing cleanup
      // errors.
    }
  }

  /** Validated immutable configuration string (SQL NULL represented by null value). */
  record StringParameter(String value) {}

  @FunctionalInterface
  interface ResultReader<T> {
    T read(ResultSet result) throws SQLException;
  }

  @FunctionalInterface
  private interface SqlCleanup extends AutoCloseable {
    @Override
    void close() throws SQLException;
  }
}
