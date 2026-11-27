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
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class SqlStringTransformTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(
            new Column("id", DataType.LONG, false),
            new Column("name", DataType.STRING, true),
            new Column("backup", DataType.STRING, true)));
  }

  private Object evaluate(String expression, String name, String backup) throws Exception {
    try (var transform = new SqlStringTransform(table(), expression, "out")) {
      transform.open();
      return transform.apply(new SeaTunnelRow(1L, name, backup)).get(0).getField(3);
    }
  }

  @Test
  void delegatesFunctionsAndNullSemanticsToHsql() throws Exception {
    assertThat(evaluate("UPPER(name)", "Ada", null)).isEqualTo("ADA");
    assertThat(evaluate("lower(name)", "Ada", null)).isEqualTo("ada");
    assertThat(evaluate("CONCAT(name,backup)", "a", "b")).isEqualTo("ab");
    assertThat(evaluate("COALESCE(name,backup)", null, "b")).isEqualTo("b");
    assertThat(evaluate("COALESCE(name,backup)", null, null)).isNull();
    assertThat(evaluate("UPPER(name)", null, "b")).isNull();
    assertThat(evaluate("UPPER(COALESCE(name,LOWER(backup)))", null, "Ada")).isEqualTo("ADA");
    assertThat(evaluate("name", "x' OR TRUE --", null)).isEqualTo("x' OR TRUE --");
  }

  @Test
  void supportsPlainStringsDoubledQuotesUnicodeBackslashesAndSqlNull() throws Exception {
    assertThat(evaluate("'Ada'", null, null)).isEqualTo("Ada");
    assertThat(evaluate("'O''Reilly'", null, null)).isEqualTo("O'Reilly");
    assertThat(evaluate("''''", null, null)).isEqualTo("'");
    assertThat(evaluate("''", null, null)).isEqualTo("");
    assertThat(evaluate("'नमस्ते'", null, null)).isEqualTo("नमस्ते");
    assertThat(evaluate("'C:\\tmp\\n'", null, null)).isEqualTo("C:\\tmp\\n");
    assertThat(evaluate("NULL", "a", null)).isNull();
    assertThat(evaluate("COALESCE(name,'unknown')", null, null)).isEqualTo("unknown");
    assertThat(evaluate("COALESCE(name,'unknown')", "Ada", null)).isEqualTo("Ada");
    assertThat(evaluate("CONCAT('user-',name)", "Ada", null)).isEqualTo("user-Ada");
    assertThat(evaluate("UPPER('Ada')", null, null)).isEqualTo("ADA");
    assertThat(evaluate("COALESCE(NULL,NULL)", null, null)).isNull();
    assertThat(evaluate("UPPER(NULL)", "x", null)).isNull();
    assertThat(evaluate("'x''; DROP TABLE t; --'", null, null)).isEqualTo("x'; DROP TABLE t; --");
  }

  @Test
  void rejectsPrefixedNonStringAndMalformedLiteralsBeforeOpening() {
    for (String expression :
        List.of(
            "N'x'",
            "E'x'",
            "U'x'",
            "R'x'",
            "B'10'",
            "RB'x'",
            "_utf8'x'",
            "q'[x]'",
            "$$x$$",
            "1",
            "TRUE",
            "DATE '2020-01-01'",
            "?",
            "'x';SELECT name",
            "'unclosed"))
      assertThatThrownBy(() -> new SqlStringTransform(table(), expression, "out"))
          .as(expression)
          .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void rejectsUnsupportedSyntaxDecorationsAndBoundsBeforeOpening() {
    for (String expression :
        List.of(
            "UPPER(id)",
            "UPPER(t.name)",
            "UPPER(unknown)",
            "UPPER(*)",
            "UPPER(DISTINCT name)",
            "UPPER(ALL name)",
            "UPPER(UNIQUE name)",
            "UPPER(name ORDER BY id)",
            "{fn UPPER(name)}",
            "UPPER(name) OVER ()",
            "LENGTH(name)",
            "name || backup",
            "?",
            "CAST(name AS VARCHAR(5))",
            "SELECT name",
            "UPPER(name); DROP TABLE t",
            "UPPER(name),LOWER(name)",
            "CONCAT(name)",
            "COALESCE(name)",
            "UPPER(name,backup)",
            "UPPER(".repeat(18) + "name" + ")".repeat(18),
            "",
            "x".repeat(4097)))
      assertThatThrownBy(() -> new SqlStringTransform(table(), expression, "out"))
          .as(expression)
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SqlStringTransform(table(), null, "out"))
        .isInstanceOf(IllegalArgumentException.class);
    for (String target : List.of("name", "not-valid", "", "x".repeat(129)))
      assertThatThrownBy(() -> new SqlStringTransform(table(), "name", target))
          .isInstanceOf(IllegalArgumentException.class);
    String leaves = String.join(",", java.util.Collections.nCopies(8, "name"));
    String tooManyNodes =
        "COALESCE("
            + String.join(",", java.util.Collections.nCopies(8, "COALESCE(" + leaves + ")"))
            + ")";
    assertThatThrownBy(() -> new SqlStringTransform(table(), tooManyNodes, "out"))
        .hasMessageContaining("AST exceeds");
    assertThatThrownBy(
            () -> new SqlStringTransform(table(), "COALESCE(" + leaves + ",name)", "out"))
        .isInstanceOf(IllegalArgumentException.class);
    var unsupported = new CatalogTable("t", List.of(new Column("d", DataType.DOUBLE, true)));
    assertThatThrownBy(() -> new SqlStringTransform(unsupported, "UPPER(d)", "out"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void preservesSchemaRowsAndLifecycleAndRecoversAfterInvalidBinding() throws Exception {
    var input = table();
    var row = new SeaTunnelRow(1L, "Ada", null);
    var transform = new SqlStringTransform(input, "UPPER(name)", "out");
    assertThat(transform.producedTable())
        .isEqualTo(
            new CatalogTable(
                "events",
                List.of(
                    new Column("id", DataType.LONG, false),
                        new Column("name", DataType.STRING, true),
                    new Column("backup", DataType.STRING, true),
                        new Column("out", DataType.STRING, true))));
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      var output = transform.apply(row);
      assertThat(output).containsExactly(new SeaTunnelRow(1L, "Ada", null, "ADA"));
      assertThat(output.get(0)).isNotSameAs(row);
      assertThat(row.getArity()).isEqualTo(3);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1, "a", null)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, "a")))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(null)).isInstanceOf(NullPointerException.class);
      assertThat(transform.apply(row).get(0).getField(3)).isEqualTo("ADA");
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused = new SqlStringTransform(input, "name", "out");
    unused.close();
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void checksInputAndResultSizeAndClearsParametersOnFailure() throws Exception {
    try (var transform = new SqlStringTransform(table(), "CONCAT(name,backup)", "out")) {
      transform.open();
      assertThatThrownBy(
              () ->
                  transform.apply(
                      new SeaTunnelRow(
                          1L, "x".repeat(SqlFilterTransform.MAX_STRING_CHARS + 1), null)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () -> transform.apply(new SeaTunnelRow(1L, "a".repeat(600000), "b".repeat(600000))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("result exceeds");
      assertThat(transform.apply(new SeaTunnelRow(1L, "a", "b")).get(0).getField(3))
          .isEqualTo("ab");
    }
  }

  @Test
  void validatesExactPluginOptions() {
    var plugin = new SqlStringPlugin();
    assertThat(plugin.identifier()).isEqualTo("sql-string");
    assertThatThrownBy(() -> plugin.createTransform(Map.of("expression", "name"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> plugin.createTransform(Map.of("expression", 1, "target", "out"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                plugin.createTransform(
                    Map.of("expression", "name", "target", "out", "sql", "bad"), table()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
