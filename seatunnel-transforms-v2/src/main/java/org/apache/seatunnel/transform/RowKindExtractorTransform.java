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
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Exposes native row metadata as a field, without changing the row's kind. */
public final class RowKindExtractorTransform implements SeaTunnelTransform {
  private final CatalogTable output;
  private final int arity;
  private boolean opened, closed;

  public RowKindExtractorTransform(CatalogTable input, String target) {
    Objects.requireNonNull(input, "input");
    if (target == null || target.length() > 128 || !target.matches("[A-Za-z_][A-Za-z0-9_]*"))
      throw new IllegalArgumentException("Expected simple row-kind target identifier");
    arity = input.columns().size();
    var columns = new ArrayList<>(input.columns());
    columns.add(new CatalogTable.Column(target, CatalogTable.DataType.STRING, false));
    output = new CatalogTable(input.name(), columns);
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
    Object[] fields = Arrays.copyOf(row.getFields(), arity + 1);
    fields[arity] = row.getRowKind().shortString();
    return List.of(row.withFields(fields));
  }

  public void close() {
    closed = true;
  }
}
