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
import java.util.List;
import java.util.Objects;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Appends one bounded nullable STRING evaluated by the embedded SQL engine. */
public final class SqlStringTransform implements SeaTunnelTransform {
  private final SqlAppendTransform delegate;

  public SqlStringTransform(CatalogTable input, String expression, String target) {
    Objects.requireNonNull(input, "input");
    delegate =
        new SqlAppendTransform(
            input,
            SqlStringPlan.compile(input, expression),
            target,
            CatalogTable.DataType.STRING,
            result -> {
              if (!result.next()) throw new SQLException("SQL string expression returned no row");
              String text = result.getString(1);
              if (text != null && text.length() > SqlRowQuery.MAX_STRING_CHARS)
                throw new IllegalArgumentException("SQL string result exceeds size limit");
              return text;
            });
  }

  public CatalogTable producedTable() {
    return delegate.producedTable();
  }

  public void open() {
    delegate.open();
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    return delegate.apply(row);
  }

  public void close() throws SQLException {
    delegate.close();
  }
}
