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
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;

/** Strict scalar mapping; JDBC/the selected driver owns the codecs. */
final class JdbcTypes {
  private JdbcTypes() {}

  static int sqlType(DataType type) {
    return switch (type) {
      case BOOLEAN -> Types.BOOLEAN;
      case INT -> Types.INTEGER;
      case LONG -> Types.BIGINT;
      case DOUBLE -> Types.DOUBLE;
      case STRING -> Types.VARCHAR;
      default -> throw JdbcConfig.bad("Unsupported JDBC schema type");
    };
  }

  static void validateMetadata(ResultSetMetaData metadata, JdbcConfig config) throws SQLException {
    if (metadata.getColumnCount() != config.catalog.columns().size())
      throw new SQLException("JDBC schema mismatch");
    for (int i = 0; i < config.catalog.columns().size(); i++) {
      Column col = config.catalog.columns().get(i);
      int actual = metadata.getColumnType(i + 1);
      boolean compatible =
          actual == sqlType(col.type())
              || (col.type() == DataType.STRING
                  && (actual == Types.CHAR || actual == Types.LONGVARCHAR))
              || (col.type() == DataType.BOOLEAN
                  && actual == Types.BIT
                  && "bool".equalsIgnoreCase(metadata.getColumnTypeName(i + 1)));
      if (config.dialect == JdbcDialect.MYSQL) {
        // Connector/J BOOLEAN aliases use TINYINT, not a portable boolean carrier.
        if (col.type() == DataType.BOOLEAN)
          compatible = actual == Types.BIT && metadata.getPrecision(i + 1) == 1;
        if (col.type() == DataType.INT
            || col.type() == DataType.LONG
            || col.type() == DataType.DOUBLE) compatible = compatible && metadata.isSigned(i + 1);
      }
      if (!compatible || !col.name().equals(metadata.getColumnLabel(i + 1)))
        throw new SQLException("JDBC schema mismatch");
    }
  }

  static Object read(ResultSet rs, int index, Column col) throws SQLException {
    Object value =
        switch (col.type()) {
          case BOOLEAN -> rs.getBoolean(index);
          case INT -> rs.getInt(index);
          case LONG -> rs.getLong(index);
          case DOUBLE -> rs.getDouble(index);
          case STRING -> rs.getString(index);
          default -> throw JdbcConfig.bad("Unsupported JDBC schema type");
        };
    if (rs.wasNull()) value = null;
    validate(value, col);
    return value;
  }

  static void validate(Object value, Column col) {
    if (value == null) {
      if (!col.nullable()) throw new IllegalArgumentException("NULL in nonnullable JDBC column");
      return;
    }
    boolean valid =
        switch (col.type()) {
          case BOOLEAN -> value instanceof Boolean;
          case INT -> value instanceof Integer;
          case LONG -> value instanceof Long;
          case DOUBLE -> value instanceof Double;
          case STRING -> value instanceof String;
          default -> false;
        };
    if (!valid) throw new IllegalArgumentException("JDBC row type mismatch");
  }

  static void bind(PreparedStatement statement, int index, Object value, Column col)
      throws SQLException {
    validate(value, col);
    if (value == null) {
      statement.setNull(index, sqlType(col.type()));
      return;
    }
    switch (col.type()) {
      case BOOLEAN -> statement.setBoolean(index, (Boolean) value);
      case INT -> statement.setInt(index, (Integer) value);
      case LONG -> statement.setLong(index, (Long) value);
      case DOUBLE -> statement.setDouble(index, (Double) value);
      case STRING -> statement.setString(index, (String) value);
      default -> throw JdbcConfig.bad("Unsupported JDBC schema type");
    }
  }
}
