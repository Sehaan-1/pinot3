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

/** Appends one bounded nullable STRING evaluated by the embedded SQL engine. */
public final class SqlStringTransform implements SeaTunnelTransform {
  private final CatalogTable output;
  private final SqlRowQuery query;

  public SqlStringTransform(CatalogTable input, String expression, String target) {
    Objects.requireNonNull(input, "input");
    if (target == null || target.length() > 128 || !target.matches("[A-Za-z_][A-Za-z0-9_]*"))
      throw new IllegalArgumentException("Expected simple SQL string target identifier");
    var columns = new ArrayList<>(input.columns());
    columns.add(new CatalogTable.Column(target, CatalogTable.DataType.STRING, true));
    output = new CatalogTable(input.name(), columns);
    query = new SqlRowQuery(input, SqlStringPlan.compile(input, expression));
  }

  public CatalogTable producedTable() {
    return output;
  }

  public void open() {
    query.open();
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    String value =
        query.evaluate(
            row,
            result -> {
              if (!result.next()) throw new SQLException("SQL string expression returned no row");
              String text = result.getString(1);
              if (text != null && text.length() > SqlRowQuery.MAX_STRING_CHARS)
                throw new IllegalArgumentException("SQL string result exceeds size limit");
              return text;
            });
    Object[] fields = Arrays.copyOf(row.getFields(), output.columns().size());
    fields[fields.length - 1] = value;
    return List.of(new SeaTunnelRow(fields));
  }

  public void close() throws SQLException {
    query.close();
  }
}
