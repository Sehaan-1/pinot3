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

class StringTransformsTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, false), new Column("text", DataType.STRING, true)));
  }

  @Test
  void replacesLiteralOccurrencesWithoutMutatingInputOrSchema() throws Exception {
    try (var transform = StringTransforms.replace(table(), "text", ".", "$")) {
      assertThat(transform.producedTable()).isEqualTo(table());
      transform.open();
      var input = new SeaTunnelRow(1L, "a.b.c");
      assertThat(transform.apply(input)).containsExactly(new SeaTunnelRow(1L, "a$b$c"));
      assertThat(input.getField(1)).isEqualTo("a.b.c");
      assertThat(transform.apply(new SeaTunnelRow(2L, null)))
          .containsExactly(new SeaTunnelRow(2L, null));
    }
    try (var transform = StringTransforms.replace(table(), "text", "aa", "")) {
      transform.open();
      assertThat(transform.apply(new SeaTunnelRow(1L, "aaa")))
          .containsExactly(new SeaTunnelRow(1L, "a"));
    }
  }

  @Test
  void splitsBoundedlyPreservingRemainderTrailingEmptiesAndNullPadding() throws Exception {
    try (var transform = StringTransforms.split(table(), "text", "|", List.of("a", "b", "c"))) {
      assertThat(transform.producedTable().columns())
          .containsExactly(
              table().columns().get(0),
              table().columns().get(1),
              new Column("a", DataType.STRING, true),
              new Column("b", DataType.STRING, true),
              new Column("c", DataType.STRING, true));
      transform.open();
      assertThat(transform.apply(new SeaTunnelRow(1L, "a|b|c|d")))
          .containsExactly(new SeaTunnelRow(1L, "a|b|c|d", "a", "b", "c|d"));
      assertThat(transform.apply(new SeaTunnelRow(1L, "a||")))
          .containsExactly(new SeaTunnelRow(1L, "a||", "a", "", ""));
      assertThat(transform.apply(new SeaTunnelRow(1L, "a")))
          .containsExactly(new SeaTunnelRow(1L, "a", "a", null, null));
      assertThat(transform.apply(new SeaTunnelRow(1L, "")))
          .containsExactly(new SeaTunnelRow(1L, "", "", null, null));
      assertThat(transform.apply(new SeaTunnelRow(1L, null)))
          .containsExactly(new SeaTunnelRow(1L, null, null, null, null));
    }
    try (var transform = StringTransforms.split(table(), "text", ".", List.of("only"))) {
      transform.open();
      assertThat(transform.apply(new SeaTunnelRow(1L, "a.b")))
          .containsExactly(new SeaTunnelRow(1L, "a.b", "a.b"));
    }
  }

  @Test
  void rejectsBadSchemasOptionsAndNamesBeforeExecution() {
    assertThatThrownBy(() -> StringTransforms.replace(table(), "id", "a", "b"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringTransforms.replace(table(), "missing", "a", "b"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringTransforms.replace(table(), "text", "", "b"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringTransforms.replace(table(), "text", "a", "b".repeat(4097)))
        .isInstanceOf(IllegalArgumentException.class);
    for (List<String> names :
        List.of(List.<String>of(), List.of("id"), List.of("a", "a"), List.of(" ")))
      assertThatThrownBy(() -> StringTransforms.split(table(), "text", "|", names))
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StringTransforms.split(table(), "text", "", List.of("a")))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                StringTransforms.split(
                    table(),
                    "text",
                    "|",
                    java.util.stream.IntStream.range(0, 129).mapToObj(i -> "f" + i).toList()))
        .isInstanceOf(IllegalArgumentException.class);
    var replace = new StringReplacePlugin();
    assertThatThrownBy(
            () ->
                replace.createTransform(
                    Map.of("field", "text", "search", "a", "replacement", 3), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                replace.createTransform(
                    Map.of("field", "text", "search", "a", "replacement", "b", "unknown", true),
                    table()))
        .isInstanceOf(IllegalArgumentException.class);
    var split = new StringSplitPlugin();
    assertThatThrownBy(
            () ->
                split.createTransform(
                    Map.of("field", "text", "separator", "|", "fields", List.of(1)), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                split.createTransform(
                    Map.of("field", "text", "separator", "|", "fields", "a"), table()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void enforcesLifecycleArityAndRuntimeTypes() throws Exception {
    for (var transform :
        List.of(
            StringTransforms.replace(table(), "text", "a", "b"),
            StringTransforms.split(table(), "text", "|", List.of("a")))) {
      try (transform) {
        assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, "a")))
            .isInstanceOf(IllegalStateException.class);
        transform.open();
        assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L)))
            .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, 3)))
            .isInstanceOf(IllegalArgumentException.class);
        transform.close();
        transform.close();
        assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, "a")))
            .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      }
    }
  }
}
