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

import java.nio.file.*;
import java.util.*;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.*;
import org.apache.seatunnel.common.exception.ConfigException;

/** Pure validation. Filesystem access belongs to runtime owners. */
final class FileConfig {
  final Path root;
  final List<String> paths;
  final String format;
  final CatalogTable table;
  final long maxFile, maxRow;

  FileConfig(Map<String, Object> options, CatalogTable input) {
    var allowed = new HashSet<>(Set.of("root", "format", "max-file-bytes", "max-row-bytes"));
    allowed.addAll(input == null ? Set.of("paths", "schema") : Set.of("path"));
    if (!allowed.containsAll(options.keySet())) throw bad("Unknown file option");
    try {
      root = Path.of(text(options.get("root")));
    } catch (RuntimeException e) {
      throw bad("Invalid file root");
    }
    if (!root.isAbsolute() || !root.equals(root.normalize()))
      throw bad("File root must be absolute and normalized");
    format = text(options.get("format"));
    if (!Set.of("csv", "json", "text", "parquet", "orc").contains(format))
      throw bad("Unknown file format");
    maxFile = number(options.getOrDefault("max-file-bytes", 67108864), 1, 1073741824);
    maxRow = number(options.getOrDefault("max-row-bytes", 1048576), 1, 16777216);
    if (input == null) {
      if (!(options.get("paths") instanceof List<?> values)
          || values.isEmpty()
          || values.size() > 128) throw bad("paths requires 1..128 relative file names");
      var result = new ArrayList<String>();
      for (Object value : values) result.add(child(text(value)));
      if (new HashSet<>(result).size() != result.size()) throw bad("Duplicate file path");
      paths = List.copyOf(result);
      if (!(options.get("schema") instanceof List<?> columns)
          || columns.isEmpty()
          || columns.size() > 128) throw bad("schema requires 1..128 columns");
      var resultColumns = new ArrayList<Column>();
      for (Object item : columns) {
        if (!(item instanceof Map<?, ?> c)
            || !c.keySet().equals(Set.of("name", "type", "nullable"))
            || !(c.get("nullable") instanceof Boolean nullable)) throw bad("Invalid file column");
        try {
          resultColumns.add(
              new Column(text(c.get("name")), DataType.valueOf(text(c.get("type"))), nullable));
        } catch (RuntimeException e) {
          throw bad("Invalid file column");
        }
      }
      table = new CatalogTable("file", resultColumns);
    } else {
      paths = List.of(child(text(options.get("path"))));
      table = input;
    }
    if (table.columns().size() > 128) throw bad("Too many file columns");
    for (Column column : table.columns())
      if (!column.name().matches("[A-Za-z_][A-Za-z0-9_]{0,127}")
          || !Set.of(
                  DataType.BOOLEAN,
                  DataType.INT,
                  DataType.LONG,
                  DataType.FLOAT,
                  DataType.DOUBLE,
                  DataType.STRING,
                  DataType.BYTES)
              .contains(column.type())) throw bad("Unsupported file column name/type");
    if (format.equals("text")
        && (table.columns().size() != 1
            || table.columns().get(0).type() != DataType.STRING
            || table.columns().get(0).nullable()))
      throw bad("Text requires one non-null STRING column");
  }

  static String child(String text) {
    try {
      var path = Path.of(text);
      if (text.isBlank()
          || path.isAbsolute()
          || text.contains("\\")
          || text.contains(":")
          || text.contains("\n")
          || text.contains("\r")) throw bad("Invalid relative file path");
      for (Path p : path)
        if (p.toString().equals("..") || p.toString().equals("."))
          throw bad("Invalid relative file path");
      if (!text.equals(path.toString())) throw bad("File path must be normalized");
      return text;
    } catch (InvalidPathException e) {
      throw bad("Invalid relative file path");
    }
  }

  static String text(Object value) {
    if (value instanceof String s && !s.isBlank()) return s;
    throw bad("Missing/invalid file string option");
  }

  static long number(Object value, long min, long max) {
    long n;
    if (value instanceof Integer i) n = i;
    else if (value instanceof Long l) n = l;
    else throw bad("File limit must be integral");
    if (n < min || n > max) throw bad("File limit out of range");
    return n;
  }

  static ConfigException bad(String message) {
    return new ConfigException(message);
  }

  static IllegalStateException failure() {
    return new IllegalStateException("Local file operation failed (details redacted)");
  }
}
