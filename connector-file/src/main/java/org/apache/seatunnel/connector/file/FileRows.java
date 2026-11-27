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

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.api.table.CatalogTable.DataType;

/** Strict logical type conversion at library boundaries. */
final class FileRows {
  private FileRows() {}

  static SeaTunnelRow validate(FileConfig config, SeaTunnelRow row) {
    if (row == null
        || row.getRowKind() != RowKind.INSERT
        || row.getArity() != config.table.columns().size()) throw FileConfig.failure();
    long bytes = 0;
    for (int i = 0; i < row.getArity(); i++) {
      Object v = row.getField(i);
      var column = config.table.columns().get(i);
      if (v == null) {
        if (!column.nullable()) throw FileConfig.failure();
        continue;
      }
      boolean valid =
          switch (column.type()) {
            case BOOLEAN -> v instanceof Boolean;
            case INT -> v instanceof Integer;
            case LONG -> v instanceof Long;
            case FLOAT -> v instanceof Float f && Float.isFinite(f);
            case DOUBLE -> v instanceof Double d && Double.isFinite(d);
            case STRING -> v instanceof String;
            case BYTES -> v instanceof byte[];
            default -> false;
          };
      if (!valid) throw FileConfig.failure();
      bytes +=
          v instanceof byte[] b ? b.length : v.toString().getBytes(StandardCharsets.UTF_8).length;
    }
    if (bytes > config.maxRow) throw FileConfig.failure();
    return row;
  }

  static Object parse(String value, DataType type) {
    try {
      return switch (type) {
        case STRING -> value;
        case BYTES -> Base64.getDecoder().decode(value);
        case BOOLEAN -> {
          if (!Set.of("true", "false").contains(value)) throw FileConfig.failure();
          yield Boolean.valueOf(value);
        }
        case INT -> Integer.valueOf(value);
        case LONG -> Long.valueOf(value);
        case FLOAT -> Float.valueOf(value);
        case DOUBLE -> Double.valueOf(value);
        default -> throw FileConfig.failure();
      };
    } catch (RuntimeException e) {
      throw FileConfig.failure();
    }
  }

  static String print(Object value) {
    return value instanceof byte[] b ? Base64.getEncoder().encodeToString(b) : value.toString();
  }
}
