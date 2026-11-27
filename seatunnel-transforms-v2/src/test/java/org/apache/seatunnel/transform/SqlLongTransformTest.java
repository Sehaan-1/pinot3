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

class SqlLongTransformTest {
  private CatalogTable table() {
    return new CatalogTable(
        "numbers",
        List.of(new Column("a", DataType.INT, true), new Column("b", DataType.LONG, true)));
  }

  private Object evaluate(String expression, Object a, Object b) throws Exception {
    try (var transform = new SqlLongTransform(table(), expression, "out")) {
      transform.open();
      return transform.apply(new SeaTunnelRow(a, b)).get(0).getField(2);
    }
  }

  @Test
  void evaluatesPromotedIntegerArithmeticPrecedenceAndNullThroughSql() throws Exception {
    assertThat(evaluate("a+b", 2, 3L)).isEqualTo(5L);
    assertThat(evaluate("a-b", 2, 3L)).isEqualTo(-1L);
    assertThat(evaluate("a*b", 2, 3L)).isEqualTo(6L);
    assertThat(evaluate("a+a", Integer.MAX_VALUE, 0L)).isEqualTo(4294967294L);
    assertThat(evaluate("a+b*b", 2, 3L)).isEqualTo(11L);
    assertThat(evaluate("(a+b)*b", 2, 3L)).isEqualTo(15L);
    assertThat(evaluate("-a + +b", 2, 3L)).isEqualTo(1L);
    assertThat(evaluate("a+b", null, 3L)).isNull();
    assertThat(evaluate("b", 2, Long.MIN_VALUE)).isEqualTo(Long.MIN_VALUE);
  }

  @Test
  void rejectsUnsupportedAstAndUnknownNonNumericOrDecoratedFields() {
    for (String expression :
        List.of(
            "a/b",
            "a%b",
            "a+1",
            "1",
            "NULL",
            "CAST(a AS BIGINT)",
            "ABS(a)",
            "x+a",
            "t.a+b",
            "a AS x",
            "a; SELECT b",
            "a,b",
            "a='x'",
            "a||b",
            "?",
            "a[1]",
            "a COMMENT 'x'",
            "-b;",
            "",
            "a".repeat(4097),
            "(".repeat(18) + "a" + ")".repeat(18)))
      assertThatThrownBy(() -> new SqlLongTransform(table(), expression, "out"))
          .as(expression)
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new SqlLongTransform(table(), null, "out"))
        .isInstanceOf(IllegalArgumentException.class);
    for (String target : List.of("a", "bad-target", "", "x".repeat(129)))
      assertThatThrownBy(() -> new SqlLongTransform(table(), "a+b", target))
          .isInstanceOf(IllegalArgumentException.class);
    String balanced = "a";
    for (int i = 0; i < 6; i++) balanced = "(" + balanced + "+" + balanced + ")";
    String tooManyNodes = balanced;
    assertThatThrownBy(() -> new SqlLongTransform(table(), tooManyNodes, "out"))
        .hasMessageContaining("AST exceeds");
    var strings = new CatalogTable("t", List.of(new Column("s", DataType.STRING, true)));
    assertThatThrownBy(() -> new SqlLongTransform(strings, "s", "out"))
        .isInstanceOf(IllegalArgumentException.class);
    var decimals = new CatalogTable("t", List.of(new Column("d", DataType.DECIMAL, true)));
    assertThatThrownBy(() -> new SqlLongTransform(decimals, "d", "out"))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void overflowFailsRatherThanWrappingAndParametersRecover() throws Exception {
    assertThatThrownBy(() -> evaluate("b+b", 1, Long.MAX_VALUE))
        .isInstanceOf(IllegalStateException.class)
        .hasCauseInstanceOf(java.sql.SQLException.class);
    assertThatThrownBy(() -> evaluate("b*b", 1, Long.MAX_VALUE))
        .isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> evaluate("-b", 1, Long.MIN_VALUE))
        .isInstanceOf(IllegalStateException.class);
    try (var transform = new SqlLongTransform(table(), "b+b", "out")) {
      transform.open();
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1, Long.MAX_VALUE)))
          .isInstanceOf(IllegalStateException.class);
      assertThat(transform.apply(new SeaTunnelRow(1, 2L)))
          .containsExactly(new SeaTunnelRow(1, 2L, 4L));
    }
  }

  @Test
  void preservesSchemaRowsAndSharedLifecycle() throws Exception {
    var transform = new SqlLongTransform(table(), "a+b", "out");
    assertThat(transform.producedTable())
        .isEqualTo(
            new CatalogTable(
                "numbers",
                List.of(
                    new Column("a", DataType.INT, true),
                    new Column("b", DataType.LONG, true),
                    new Column("out", DataType.LONG, true))));
    var row = new SeaTunnelRow(1, 2L);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, 2L)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(null)).isInstanceOf(NullPointerException.class);
      var output = transform.apply(row);
      assertThat(output).containsExactly(new SeaTunnelRow(1, 2L, 3L));
      assertThat(output.get(0)).isNotSameAs(row);
      assertThat(row).isEqualTo(new SeaTunnelRow(1, 2L));
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused = new SqlLongTransform(table(), "a+b", "out");
    unused.close();
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void validatesPluginOptions() {
    var plugin = new SqlLongPlugin();
    assertThat(plugin.identifier()).isEqualTo("sql-long");
    assertThatThrownBy(() -> plugin.createTransform(Map.of("expression", "a+b"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () -> plugin.createTransform(Map.of("expression", 1, "target", "out"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                plugin.createTransform(
                    Map.of("expression", "a+b", "target", "out", "extra", "x"), table()))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
