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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Compiles exact schema names to positional mappings; no expression language or parser. */
public final class FieldTransforms {
  private FieldTransforms() {}

  public static SeaTunnelTransform rename(CatalogTable input, Map<String, String> fields) {
    return create(input, fields, false);
  }

  /** Mapping is target -> existing input source. Appended targets are lexically ordered. */
  public static SeaTunnelTransform copy(CatalogTable input, Map<String, String> fields) {
    return create(input, fields, true);
  }

  private static SeaTunnelTransform create(
      CatalogTable input, Map<String, String> fields, boolean copy) {
    Objects.requireNonNull(input, "input");
    Map<String, String> mappings = Map.copyOf(fields);
    if (mappings.isEmpty() || mappings.size() > 128)
      throw new IllegalArgumentException("Expected 1..128 field mappings");
    List<Column> columns = new ArrayList<>();
    List<Integer> positions = new ArrayList<>();
    for (int i = 0; i < input.columns().size(); i++) {
      Column column = input.columns().get(i);
      String name = copy ? column.name() : mappings.getOrDefault(column.name(), column.name());
      columns.add(new Column(name, column.type(), column.nullable()));
      positions.add(i);
    }
    for (var entry : new TreeMap<>(mappings).entrySet()) {
      String source = copy ? entry.getValue() : entry.getKey();
      int index = -1;
      for (int i = 0; i < input.columns().size(); i++)
        if (input.columns().get(i).name().equals(source)) {
          index = i;
          break;
        }
      if (index < 0) throw new IllegalArgumentException("Unknown source field: " + source);
      if (copy) {
        Column column = input.columns().get(index);
        columns.add(new Column(entry.getKey(), column.type(), column.nullable()));
        positions.add(index);
      }
    }
    CatalogTable output = new CatalogTable(input.name(), columns);
    int[] projection = positions.stream().mapToInt(Integer::intValue).toArray();
    return new Mapping(input.columns().size(), output, projection);
  }

  /** Internal reuse for validated library-backed projection plans. */
  static SeaTunnelTransform project(CatalogTable input, CatalogTable output, int[] positions) {
    return new Mapping(input.columns().size(), output, positions.clone());
  }

  private static final class Mapping implements SeaTunnelTransform {
    private final int arity;
    private final CatalogTable output;
    private final int[] projection;
    private boolean opened, closed;

    Mapping(int arity, CatalogTable output, int[] projection) {
      this.arity = arity;
      this.output = output;
      this.projection = projection;
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
      Object[] values = new Object[projection.length];
      for (int i = 0; i < values.length; i++) values[i] = row.getField(projection[i]);
      return List.of(row.withFields(values));
    }

    public void close() {
      closed = true;
    }
  }
}
