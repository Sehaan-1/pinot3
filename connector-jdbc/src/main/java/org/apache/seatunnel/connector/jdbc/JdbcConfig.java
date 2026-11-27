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

import java.math.BigInteger;
import java.net.URI;
import java.util.*;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.common.exception.ConfigException;

/** Validated credentials/configuration. Deliberately no generated secret-bearing toString. */
final class JdbcConfig {
  final String url, user, password, schema, table, sslMode, partitionColumn;
  final CatalogTable catalog;
  final int fetchSize, batchSize, timeout;
  final List<JdbcSource.Split> splits;

  JdbcConfig(Map<String, Object> options, CatalogTable input) {
    var allowed =
        new HashSet<>(
            Set.of(
                "url", "user", "password", "schema", "table", "ssl-mode", "query-timeout-seconds"));
    allowed.addAll(
        input == null
            ? Set.of(
                "columns",
                "fetch-size",
                "partition-column",
                "lower-bound",
                "upper-bound",
                "partitions")
            : Set.of("batch-size"));
    if (!allowed.containsAll(options.keySet())) throw bad("Unknown JDBC option");
    url = text(options, "url");
    user = text(options, "user");
    password = text(options, "password");
    try {
      if (!url.startsWith("jdbc:postgresql://")) throw new IllegalArgumentException();
      URI uri = URI.create(url.substring(5));
      if (uri.getHost() == null
          || uri.getUserInfo() != null
          || uri.getRawQuery() != null
          || uri.getRawFragment() != null
          || uri.getPort() == 0
          || uri.getPort() > 65535
          || uri.getRawPath() == null
          || !uri.getRawPath().matches("/[A-Za-z0-9_-]{1,63}"))
        throw new IllegalArgumentException();
    } catch (IllegalArgumentException failure) {
      throw bad("Invalid PostgreSQL URL (no properties or credentials allowed)");
    }
    schema = identifier(text(options, "schema"));
    table = identifier(text(options, "table"));
    sslMode = options.containsKey("ssl-mode") ? text(options, "ssl-mode") : "verify-full";
    if (!Set.of("verify-full", "require", "disable").contains(sslMode))
      throw bad("Invalid ssl-mode");
    timeout = integer(options, "query-timeout-seconds", 30, 1, 300);
    fetchSize = integer(options, "fetch-size", 500, 1, 10000);
    batchSize = integer(options, "batch-size", 100, 1, 1000);
    if (input == null) {
      if (!(options.get("columns") instanceof List<?> columns)
          || columns.isEmpty()
          || columns.size() > 128) throw bad("columns requires 1..128 entries");
      var result = new ArrayList<Column>();
      for (Object raw : columns) {
        if (!(raw instanceof Map<?, ?> col)
            || !col.keySet().equals(Set.of("name", "type", "nullable")))
          throw bad("Invalid column definition");
        if (!(col.get("name") instanceof String name)
            || !(col.get("type") instanceof String type)
            || !(col.get("nullable") instanceof Boolean nullable))
          throw bad("Invalid column types");
        DataType dataType;
        try {
          dataType = DataType.valueOf(type);
        } catch (IllegalArgumentException e) {
          throw bad("Unsupported JDBC type");
        }
        JdbcTypes.sqlType(dataType);
        result.add(new Column(identifier(name), dataType, nullable));
      }
      catalog = new CatalogTable(table, result);
    } else {
      if (input.columns().size() > 128) throw bad("At most 128 columns supported");
      input
          .columns()
          .forEach(
              c -> {
                identifier(c.name());
                JdbcTypes.sqlType(c.type());
              });
      catalog = input;
    }
    var partitionKeys = Set.of("partition-column", "lower-bound", "upper-bound", "partitions");
    long present = partitionKeys.stream().filter(options::containsKey).count();
    if (present != 0 && present != 4) throw bad("Partition options must be specified together");
    var ranges = new ArrayList<JdbcSource.Split>();
    if (present == 0) {
      partitionColumn = null;
      ranges.add(new JdbcSource.Split(0, null, null));
    } else {
      partitionColumn = identifier(text(options, "partition-column"));
      Column col =
          catalog.columns().stream()
              .filter(c -> c.name().equals(partitionColumn))
              .findFirst()
              .orElseThrow(() -> bad("Unknown partition column"));
      if (col.type() != DataType.INT && col.type() != DataType.LONG)
        throw bad("Partition column must be INT or LONG");
      long lower = number(options, "lower-bound"), upper = number(options, "upper-bound");
      if (col.type() == DataType.INT && (lower < Integer.MIN_VALUE || upper > Integer.MAX_VALUE))
        throw bad("INT partition bounds out of range");
      int count = integer(options, "partitions", 2, 2, 32);
      BigInteger start = BigInteger.valueOf(lower),
          width = BigInteger.valueOf(upper).subtract(start);
      if (width.compareTo(BigInteger.valueOf(count)) < 0)
        throw bad("Partition range too small or reversed");
      for (int i = 0; i < count; i++)
        ranges.add(
            new JdbcSource.Split(
                i,
                start
                    .add(width.multiply(BigInteger.valueOf(i)).divide(BigInteger.valueOf(count)))
                    .longValueExact(),
                start
                    .add(
                        width.multiply(BigInteger.valueOf(i + 1)).divide(BigInteger.valueOf(count)))
                    .longValueExact()));
    }
    splits = List.copyOf(ranges);
  }

  String relation() {
    return quote(schema) + "." + quote(table);
  }

  String columnSql() {
    return String.join(",", catalog.columns().stream().map(c -> quote(c.name())).toList());
  }

  static String quote(String value) {
    return "\"" + identifier(value) + "\"";
  }

  static String identifier(String value) {
    if (!value.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")) throw bad("Invalid SQL identifier");
    return value;
  }

  static String text(Map<String, Object> options, String key) {
    if (!(options.get(key) instanceof String value))
      throw bad("Missing or invalid JDBC option: " + key);
    return value;
  }

  static long number(Map<String, Object> options, String key) {
    Object value = options.get(key);
    if (value instanceof Integer i) return i.longValue();
    if (value instanceof Long l) return l;
    throw bad("Invalid integral option: " + key);
  }

  static int integer(Map<String, Object> options, String key, int fallback, int min, int max) {
    if (!options.containsKey(key)) return fallback;
    long value = number(options, key);
    if (value < min || value > max) throw bad("JDBC option out of range: " + key);
    return (int) value;
  }

  static ConfigException bad(String message) {
    return new ConfigException(message);
  }
}
