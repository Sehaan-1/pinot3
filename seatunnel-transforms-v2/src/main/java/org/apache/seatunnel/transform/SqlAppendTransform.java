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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Shared schema/row ownership for one-column SQL append adapters. */
final class SqlAppendTransform implements SeaTunnelTransform {
  private final CatalogTable output;
  private final SqlRowQuery query;
  private final SqlRowQuery.ResultReader<?> reader;

  SqlAppendTransform(
      CatalogTable input,
      String sql,
      String target,
      CatalogTable.DataType type,
      SqlRowQuery.ResultReader<?> reader) {
    Objects.requireNonNull(input, "input");
    if (target == null || target.length() > 128 || !target.matches("[A-Za-z_][A-Za-z0-9_]*"))
      throw new IllegalArgumentException("Expected simple SQL target identifier");
    var columns = new ArrayList<>(input.columns());
    columns.add(new CatalogTable.Column(target, type, true));
    output = new CatalogTable(input.name(), columns);
    query = new SqlRowQuery(input, sql);
    this.reader = Objects.requireNonNull(reader, "reader");
  }

  public CatalogTable producedTable() {
    return output;
  }

  public void open() {
    query.open();
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    Object value = query.evaluate(row, reader);
    Object[] fields = Arrays.copyOf(row.getFields(), output.columns().size());
    fields[fields.length - 1] = value;
    return List.of(row.withFields(fields));
  }

  public void close() throws SQLException {
    query.close();
  }
}
