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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Literal string operators built on JDK string operations, never user-supplied regex. */
public final class StringTransforms {
  private StringTransforms() {}

  public static SeaTunnelTransform replace(
      CatalogTable input, String field, String search, String replacement) {
    int index = stringField(input, field);
    token(search, false);
    token(replacement, true);
    return new Operator(input, index, input) {
      SeaTunnelRow convert(SeaTunnelRow row, String value) {
        Object[] result = row.getFields();
        result[index] = value == null ? null : value.replace(search, replacement);
        return new SeaTunnelRow(result);
      }
    };
  }

  public static SeaTunnelTransform split(
      CatalogTable input, String field, String separator, List<String> fields) {
    int index = stringField(input, field);
    token(separator, false);
    List<String> names = List.copyOf(fields);
    if (names.isEmpty() || names.size() > 128)
      throw new IllegalArgumentException("Expected 1..128 split fields");
    List<Column> columns = new ArrayList<>(input.columns());
    for (String name : names) columns.add(new Column(name, DataType.STRING, true));
    CatalogTable output = new CatalogTable(input.name(), columns);
    String literal = Pattern.quote(separator);
    return new Operator(input, index, output) {
      SeaTunnelRow convert(SeaTunnelRow row, String value) {
        Object[] result = Arrays.copyOf(row.getFields(), columns.size());
        if (value != null) {
          String[] tokens = value.split(literal, names.size());
          System.arraycopy(tokens, 0, result, row.getArity(), tokens.length);
        }
        return new SeaTunnelRow(result);
      }
    };
  }

  private static int stringField(CatalogTable input, String field) {
    Objects.requireNonNull(input, "input");
    Objects.requireNonNull(field, "field");
    for (int i = 0; i < input.columns().size(); i++) {
      Column column = input.columns().get(i);
      if (column.name().equals(field)) {
        if (column.type() != DataType.STRING)
          throw new IllegalArgumentException("Field must have STRING type: " + field);
        return i;
      }
    }
    throw new IllegalArgumentException("Unknown source field: " + field);
  }

  private static void token(String value, boolean emptyAllowed) {
    if (value == null || (!emptyAllowed && value.isEmpty()) || value.length() > 4096)
      throw new IllegalArgumentException("Invalid literal string option");
  }

  private abstract static class Operator implements SeaTunnelTransform {
    private final int arity, index;
    private final CatalogTable output;
    private boolean opened, closed;

    Operator(CatalogTable input, int index, CatalogTable output) {
      arity = input.columns().size();
      this.index = index;
      this.output = output;
    }

    public CatalogTable producedTable() {
      return output;
    }

    public void open() {
      if (opened || closed)
        throw new IllegalStateException("Transform cannot be opened twice or after close");
      opened = true;
    }

    public List<SeaTunnelRow> apply(SeaTunnelRow row) {
      if (!opened || closed) throw new IllegalStateException("Transform is not open");
      if (Objects.requireNonNull(row, "row").getArity() != arity)
        throw new IllegalArgumentException("Input arity mismatch");
      Object value = row.getField(index);
      if (value != null && !(value instanceof String))
        throw new IllegalArgumentException("Expected String field value");
      return List.of(convert(row, (String) value));
    }

    abstract SeaTunnelRow convert(SeaTunnelRow row, String value);

    public void close() {
      closed = true;
    }
  }
}
