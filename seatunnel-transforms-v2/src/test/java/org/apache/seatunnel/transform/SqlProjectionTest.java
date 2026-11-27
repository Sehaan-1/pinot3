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

class SqlProjectionTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, false), new Column("name", DataType.STRING, true)));
  }

  @Test
  void selectsOrdersAliasesAndPreservesMetadataWithoutChangingInput() throws Exception {
    var input = table();
    var row = new SeaTunnelRow(2L, "Ada");
    try (var transform = SqlProjection.create(input, "name AS label, id, name AS again")) {
      assertThat(transform.producedTable())
          .isEqualTo(
              new CatalogTable(
                  "events",
                  List.of(
                      new Column("label", DataType.STRING, true),
                      new Column("id", DataType.LONG, false),
                      new Column("again", DataType.STRING, true))));
      transform.open();
      var output = transform.apply(row);
      assertThat(output).containsExactly(new SeaTunnelRow("Ada", 2L, "Ada"));
      assertThat(output.get(0)).isNotSameAs(row);
      assertThat(row).isEqualTo(new SeaTunnelRow(2L, "Ada"));
      assertThat(transform.apply(new SeaTunnelRow(2L, null)))
          .containsExactly(new SeaTunnelRow(null, 2L, null));
    }
  }

  @Test
  void supportsDroppingAndAllSourceTypesWithoutSqlCoercion() throws Exception {
    for (var type : DataType.values()) {
      var input =
          new CatalogTable(
              "typed",
              List.of(new Column("value", type, true), new Column("drop", DataType.INT, false)));
      byte[] mutable = {1, 2};
      try (var transform = SqlProjection.create(input, "value AS copied")) {
        assertThat(transform.producedTable().columns())
            .containsExactly(new Column("copied", type, true));
        transform.open();
        assertThat(transform.apply(new SeaTunnelRow(mutable, 1)).get(0).getField(0))
            .isSameAs(mutable);
      }
    }
  }

  @Test
  void parsesWholeListAndRejectsUnsupportedAstOrAliases() {
    for (String columns :
        List.of(
            "*",
            "t.*",
            "t.id",
            "id+1",
            "UPPER(name)",
            "1",
            "NULL",
            "id AS x, name AS x",
            "id,id",
            "unknown",
            "ID",
            "id implicit",
            "id AS x(a)",
            "id[1]",
            "id COMMENT 'x'",
            "(SELECT id)",
            "SELECT id",
            "id FROM t",
            "id WHERE id=1",
            "id ORDER BY id",
            "id;",
            "id; DROP TABLE t",
            "id,",
            "id AS \"x\"",
            "\"id\"",
            "",
            "a".repeat(4097)))
      assertThatThrownBy(() -> SqlProjection.create(table(), columns))
          .as(columns)
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> SqlProjection.create(table(), null))
        .isInstanceOf(IllegalArgumentException.class);
    String tooMany =
        java.util.stream.IntStream.range(0, 129)
            .mapToObj(i -> "id AS x" + i)
            .collect(java.util.stream.Collectors.joining(","));
    assertThatThrownBy(() -> SqlProjection.create(table(), tooMany)).hasMessageContaining("1..128");
    assertThatThrownBy(() -> SqlProjection.create(table(), "id AS " + "x".repeat(129)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void acceptsLibraryCommentsAndEnforcesOptionsAndLifecycle() throws Exception {
    var plugin = new SqlProjectPlugin();
    assertThat(plugin.identifier()).isEqualTo("sql-project");
    assertThatThrownBy(() -> plugin.createTransform(Map.of(), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createTransform(Map.of("columns", 1), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createTransform(Map.of("columns", "id", "extra", "x"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    var transform =
        plugin.createTransform(Map.of("columns", "/* list */ id AS key -- comment"), table());
    var row = new SeaTunnelRow(1L, "a");
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transform.apply(null)).isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(transform.apply(row)).containsExactly(new SeaTunnelRow(1L));
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused = SqlProjection.create(table(), "id");
    unused.close();
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }
}
