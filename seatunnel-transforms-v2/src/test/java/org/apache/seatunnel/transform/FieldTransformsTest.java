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
import org.apache.seatunnel.plugin.PluginDiscovery;
import org.apache.seatunnel.plugin.TransformPlugin;
import org.junit.jupiter.api.Test;

class FieldTransformsTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, false), new Column("name", DataType.STRING, true)));
  }

  @Test
  void simultaneousRenamePreservesPositionsTypesAndNulls() throws Exception {
    try (var transform = FieldTransforms.rename(table(), Map.of("id", "name", "name", "id"))) {
      assertThat(transform.producedTable().name()).isEqualTo("events");
      assertThat(transform.producedTable().columns())
          .containsExactly(
              new Column("name", DataType.LONG, false), new Column("id", DataType.STRING, true));
      transform.open();
      var input = new SeaTunnelRow(12L, null);
      assertThat(transform.apply(input)).containsExactly(new SeaTunnelRow(12L, null));
      assertThat(input.getField(0)).isEqualTo(12L);
    }
  }

  @Test
  void copyAppendsDeterministicallyAndRetainsInput() throws Exception {
    var fields = new java.util.LinkedHashMap<String, String>();
    fields.put("z", "id");
    fields.put("a", "name");
    try (var transform = FieldTransforms.copy(table(), fields)) {
      fields.clear();
      transform.open();
      assertThat(transform.producedTable().columns())
          .extracting(Column::name)
          .containsExactly("id", "name", "a", "z");
      assertThat(transform.producedTable().columns().get(2).nullable()).isTrue();
      var row = new SeaTunnelRow(7L, "seven");
      assertThat(transform.apply(row)).containsExactly(new SeaTunnelRow(7L, "seven", "seven", 7L));
      assertThat(row.getArity()).isEqualTo(2);
    }
  }

  @Test
  void rejectsUnknownFieldsCollisionsAndInvalidOptions() {
    assertThatThrownBy(() -> FieldTransforms.rename(table(), Map.of("missing", "x")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FieldTransforms.rename(table(), Map.of("id", "name")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FieldTransforms.rename(table(), Map.of("id", " ")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FieldTransforms.copy(table(), Map.of("id", "name")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FieldTransforms.copy(table(), Map.of("new", "missing")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> FieldTransforms.copy(table(), Map.of()))
        .isInstanceOf(IllegalArgumentException.class);
    var plugin = new FieldCopyPlugin();
    assertThatThrownBy(() -> plugin.createTransform(Map.of("unknown", true), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createTransform(Map.of("fields", Map.of("new", 3)), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> plugin.createTransform(Map.of("fields", List.of("id")), table()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void lifecycleAndArityAreExplicit() throws Exception {
    var transform = FieldTransforms.copy(table(), Map.of("copy", "id"));
    assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, "x")))
        .isInstanceOf(IllegalStateException.class);
    transform.open();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L)))
        .isInstanceOf(IllegalArgumentException.class);
    transform.close();
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, "x")))
        .isInstanceOf(IllegalStateException.class);
    var unopened = FieldTransforms.rename(table(), Map.of("id", "renamed"));
    unopened.close();
    assertThatThrownBy(unopened::open).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void discoversRealProviders() throws Exception {
    try (var plugins = PluginDiscovery.fromClasspath(TransformPlugin.class)) {
      assertThat(plugins.identifiers()).containsExactly("field-copy", "field-rename");
      try (var transform =
          plugins
              .get("field-copy")
              .createTransform(Map.of("fields", Map.of("copy", "id")), table())) {
        transform.open();
        assertThat(transform.apply(new SeaTunnelRow(1L, "one")))
            .containsExactly(new SeaTunnelRow(1L, "one", 1L));
      }
    }
  }
}
