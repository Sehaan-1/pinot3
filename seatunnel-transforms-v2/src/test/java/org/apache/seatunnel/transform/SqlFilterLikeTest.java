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
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class SqlFilterLikeTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, true), new Column("name", DataType.STRING, true)));
  }

  private boolean matches(String condition, String name) throws Exception {
    try (var transform = new SqlFilterTransform(table(), condition)) {
      transform.open();
      var row = new SeaTunnelRow(1L, name);
      var result = transform.apply(row);
      if (!result.isEmpty()) assertThat(result.get(0)).isSameAs(row);
      return !result.isEmpty();
    }
  }

  @Test
  void delegatesWildcardsNegationEmptyUnicodeAndNullToSql() throws Exception {
    assertThat(matches("name LIKE 'row-%'", "row-10")).isTrue();
    assertThat(matches("name LIKE 'row-%'", "other")).isFalse();
    assertThat(matches("name LIKE 'a_c'", "abc")).isTrue();
    assertThat(matches("name LIKE 'a_c'", "ac")).isFalse();
    assertThat(matches("name NOT LIKE 'row-%'", "other")).isTrue();
    assertThat(matches("name NOT LIKE '%'", null)).isFalse();
    assertThat(matches("NOT (name LIKE '%')", null)).isFalse();
    assertThat(matches("name LIKE '%'", null)).isFalse();
    assertThat(matches("name LIKE NULL", "Ada")).isFalse();
    assertThat(matches("NULL LIKE '%'", "Ada")).isFalse();
    assertThat(matches("name LIKE ''", "")).isTrue();
    assertThat(matches("name LIKE 'न%'", "नमस्ते")).isTrue();
  }

  @Test
  void acceptsSingleCharacterEscapesDoubledQuotesAndOrdinaryBackslashes() throws Exception {
    assertThat(matches("name LIKE '100!%' ESCAPE '!'", "100%")).isTrue();
    assertThat(matches("name LIKE '100!%' ESCAPE '!'", "100x")).isFalse();
    assertThat(matches("name LIKE 'a!_b' ESCAPE '!'", "a_b")).isTrue();
    assertThat(matches("name LIKE 'a!!b' ESCAPE '!'", "a!b")).isTrue();
    assertThat(matches("name LIKE 'O''Reilly'", "O'Reilly")).isTrue();
    assertThat(matches("name LIKE 'C:\\tmp'", "C:\\tmp")).isTrue();
    assertThat(matches("name LIKE 'a''%' ESCAPE ''''", "a%")).isTrue();
  }

  @Test
  void rejectsRegexDialectsDynamicPatternsInvalidEscapesAndOversizePatterns() {
    for (String condition :
        List.of(
            "id LIKE '%'",
            "name LIKE name",
            "name ILIKE '%'",
            "name RLIKE '.*'",
            "name REGEXP '.*'",
            "name LIKE BINARY '%'",
            "name LIKE N'%'",
            "name LIKE '%' ESCAPE ''",
            "name LIKE '%' ESCAPE 'ab'",
            "name LIKE '%' ESCAPE name",
            "name LIKE '%' ESCAPE NULL",
            "name LIKE '%' ESCAPE '😀'",
            "name LIKE UPPER(name)",
            "name LIKE (SELECT name)",
            "name LIKE '%'; SELECT id"))
      assertThatThrownBy(() -> new SqlFilterTransform(table(), condition))
          .as(condition)
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> new SqlFilterTransform(table(), "name LIKE '" + "x".repeat(1025) + "'"))
        .hasMessageContaining("1024");
  }

  @Test
  void acceptsPatternBoundaryAndRebindsMultiplePredicatesOnEveryRow() throws Exception {
    assertThat(matches("name LIKE '" + "x".repeat(1024) + "'", "x".repeat(1024))).isTrue();
    try (var transform =
        new SqlFilterTransform(table(), "name LIKE 'a!%' ESCAPE '!' OR name LIKE 'b_'")) {
      transform.open();
      assertThat(transform.apply(new SeaTunnelRow(1L, "a%"))).hasSize(1);
      assertThat(transform.apply(new SeaTunnelRow(1L, "b1"))).hasSize(1);
      assertThat(transform.apply(new SeaTunnelRow(1L, null))).isEmpty();
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1, "a%")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(transform.apply(new SeaTunnelRow(1L, "other"))).isEmpty();
      assertThat(transform.apply(new SeaTunnelRow(1L, "a%"))).hasSize(1);
    }
  }
}
