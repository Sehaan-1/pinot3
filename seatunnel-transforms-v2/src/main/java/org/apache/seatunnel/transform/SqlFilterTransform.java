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

/** SQL truth/null semantics are delegated to an owned embedded VALUES-row query. */
public final class SqlFilterTransform implements SeaTunnelTransform {
  public static final int MAX_STRING_CHARS = SqlRowQuery.MAX_STRING_CHARS;
  private final CatalogTable input;
  private final SqlRowQuery query;

  public SqlFilterTransform(CatalogTable input, String condition) {
    this.input = Objects.requireNonNull(input, "input");
    var plan = SqlFilterPlan.compile(input, condition);
    query = new SqlRowQuery(input, plan.sql(), plan.parameters());
  }

  public CatalogTable producedTable() {
    return input;
  }

  public void open() {
    query.open();
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    return query.evaluate(row, result -> result.next() ? List.of(row) : List.of());
  }

  public void close() throws SQLException {
    query.close();
  }
}
