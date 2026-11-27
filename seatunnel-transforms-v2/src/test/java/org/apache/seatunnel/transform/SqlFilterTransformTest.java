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

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class SqlFilterTransformTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(
            new CatalogTable.Column("id", CatalogTable.DataType.LONG, false),
            new CatalogTable.Column("name", CatalogTable.DataType.STRING, true),
            new CatalogTable.Column("active", CatalogTable.DataType.BOOLEAN, true)));
  }

  private boolean matches(String condition, Object... values) throws Exception {
    try (var transform = new SqlFilterTransform(table(), condition)) {
      transform.open();
      var row = new SeaTunnelRow(values);
      var result = transform.apply(row);
      if (!result.isEmpty()) assertThat(result.get(0)).isSameAs(row);
      return !result.isEmpty();
    }
  }

  @Test
  void delegatesThreeValuedTruthAndBoundDataToSqlEngine() throws Exception {
    assertThat(matches("id >= 10 AND name <> 'x'", 10L, "yes", true)).isTrue();
    assertThat(matches("id >= 10 AND name <> 'x'", 9L, "yes", true)).isFalse();
    assertThat(matches("name = 'x'", 10L, null, true)).isFalse();
    assertThat(matches("NOT (name = 'x')", 10L, null, true)).isFalse();
    assertThat(matches("name IS NULL", 10L, null, true)).isTrue();
    assertThat(matches("name IS NOT NULL", 10L, null, true)).isFalse();
    assertThat(matches("name = 'x' OR id = 10", 10L, null, true)).isTrue();
    assertThat(matches("active = TRUE", 10L, "a", true)).isTrue();
    assertThat(matches("active = TRUE", 10L, "a", null)).isFalse();
    assertThat(matches("active", 10L, "a", true)).isTrue();
    assertThat(matches("NULL", 10L, "a", true)).isFalse();
    assertThat(matches("name = 'O''Reilly'", 10L, "O'Reilly", true)).isTrue();
    assertThat(matches("name = 'x'", 10L, "x' OR TRUE --", true)).isFalse();
    assertThat(matches("id = -1", -1L, "a", false)).isTrue();
  }

  @Test
  void rejectsEverythingOutsideTheAllowlistAndIncompleteInput() {
    for (String sql :
        List.of(
            "id > 1; DROP TABLE x",
            "EXISTS (SELECT 1)",
            "LENGTH(name) > 0",
            "id + 1 > 2",
            "input.id > 1",
            "unknown=1",
            "name LIKE '%'",
            "id = ?",
            "",
            "x".repeat(4097)))
      assertThatThrownBy(() -> new SqlFilterTransform(table(), sql))
          .isInstanceOf(IllegalArgumentException.class);
    var plugin = new SqlFilterPlugin();
    assertThatThrownBy(() -> plugin.createTransform(Map.of("sql", "id=1"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createTransform(Map.of("condition", 1), table()))
        .isInstanceOf(IllegalArgumentException.class);
    var unsupported =
        new CatalogTable(
            "t", List.of(new CatalogTable.Column("d", CatalogTable.DataType.DOUBLE, true)));
    assertThatThrownBy(() -> new SqlFilterTransform(unsupported, "d=1"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void validatesLifecycleAndClearsParametersEvenAfterBindingFailure() throws Exception {
    var input = table();
    var transform = new SqlFilterTransform(input, "id > 1");
    var row = new SeaTunnelRow(2L, "a", true);
    assertThat(transform.producedTable()).isSameAs(input);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(2, "a", true)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(2L, "a")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  transform.apply(
                      new SeaTunnelRow(
                          2L, "a".repeat(SqlFilterTransform.MAX_STRING_CHARS + 1), true)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(transform.apply(row)).containsExactly(row);
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused = new SqlFilterTransform(input, "id > 1");
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }
}
