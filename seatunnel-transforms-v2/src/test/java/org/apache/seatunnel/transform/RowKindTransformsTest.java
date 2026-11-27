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
import org.apache.seatunnel.api.table.RowKind;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.junit.jupiter.api.Test;

class RowKindTransformsTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(
            new Column("id", DataType.LONG, false),
            new Column("name", DataType.STRING, true),
            new Column("json", DataType.STRING, true)));
  }

  @Test
  void allExistingTransformsPreserveAllNativeKinds() throws Exception {
    var input = table();
    for (var kind : RowKind.values()) {
      var row = SeaTunnelRow.ofKind(kind, 1L, "a|b", "{\"x\":\"Ada\"}");
      for (SeaTunnelTransform transform :
          List.of(
              FieldTransforms.rename(input, Map.of("name", "label")),
              FieldTransforms.copy(input, Map.of("copy", "name")),
              SqlProjection.create(input, "name AS label,id"),
              StringTransforms.replace(input, "name", "a", "x"),
              StringTransforms.split(input, "name", "|", List.of("first", "second")),
              new JsonPathTransform(input, "json", "$.x", "extracted"),
              new SqlStringTransform(input, "UPPER(name)", "upper"),
              new SqlLongTransform(input, "id*10", "scaled"),
              new SqlFilterTransform(input, "id=1"),
              new RowKindExtractorTransform(input, "op"))) {
        try (transform) {
          transform.open();
          var result = transform.apply(row);
          assertThat(result).hasSize(1);
          assertThat(result.get(0).getRowKind()).isEqualTo(kind);
        }
      }
      assertThat(row).isEqualTo(SeaTunnelRow.ofKind(kind, 1L, "a|b", "{\"x\":\"Ada\"}"));
    }
  }

  @Test
  void extractsActualMetadataAndPreservesSchemaAndOwnership() throws Exception {
    for (var kind : RowKind.values())
      try (var transform = new RowKindExtractorTransform(table(), "op")) {
        assertThat(transform.producedTable())
            .isEqualTo(
                new CatalogTable(
                    "events",
                    List.of(
                        new Column("id", DataType.LONG, false),
                        new Column("name", DataType.STRING, true),
                        new Column("json", DataType.STRING, true),
                        new Column("op", DataType.STRING, false))));
        transform.open();
        var row = SeaTunnelRow.ofKind(kind, 1L, null, null);
        var result = transform.apply(row);
        assertThat(result)
            .containsExactly(SeaTunnelRow.ofKind(kind, 1L, null, null, kind.shortString()));
        assertThat(result.get(0)).isNotSameAs(row);
        assertThat(row.getArity()).isEqualTo(3);
      }
  }

  @Test
  void validatesOptionsTargetArityAndLifecycle() throws Exception {
    var plugin = new RowKindExtractorPlugin();
    assertThat(plugin.identifier()).isEqualTo("row-kind-extractor");
    for (Map<String, Object> options :
        List.of(
            Map.<String, Object>of(),
            Map.<String, Object>of("target", 1),
            Map.<String, Object>of("target", "op", "extra", "x")))
      assertThatThrownBy(() -> plugin.createTransform(options, table()))
          .isInstanceOf(IllegalArgumentException.class);
    for (String target : List.of("id", "", "bad-target", "x".repeat(129)))
      assertThatThrownBy(() -> new RowKindExtractorTransform(table(), target))
          .isInstanceOf(IllegalArgumentException.class);
    var row = new SeaTunnelRow(1L, null, null);
    var transform = plugin.createTransform(Map.of("target", "op"), table());
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(null)).isInstanceOf(NullPointerException.class);
      assertThat(transform.apply(row).get(0).getField(3)).isEqualTo("+I");
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused = new RowKindExtractorTransform(table(), "op");
    unused.close();
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }
}
