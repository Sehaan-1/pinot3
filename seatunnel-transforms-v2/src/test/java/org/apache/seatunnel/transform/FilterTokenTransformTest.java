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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.RowKind;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class FilterTokenTransformTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, false), new Column("tags", DataType.STRING, true)));
  }

  private boolean matches(
      String value, String delimiter, List<String> tokens, FilterTokenTransform.MatchMode mode)
      throws Exception {
    try (var transform = new FilterTokenTransform(table(), "tags", delimiter, tokens, mode)) {
      transform.open();
      var row = SeaTunnelRow.ofKind(RowKind.DELETE, 1L, value);
      var result = transform.apply(row);
      if (!result.isEmpty()) assertThat(result.get(0)).isSameAs(row);
      return !result.isEmpty();
    }
  }

  @Test
  void evaluatesAnyAllAsExactUntrimmedCaseSensitiveMembership() throws Exception {
    var any = FilterTokenTransform.MatchMode.ANY;
    var all = FilterTokenTransform.MatchMode.ALL;
    assertThat(matches("a,b", ",", List.of("b"), any)).isTrue();
    assertThat(matches("abc,b", ",", List.of("a"), any)).isFalse();
    assertThat(matches("a,b", ",", List.of("x", "b"), any)).isTrue();
    assertThat(matches("b,a", ",", List.of("a", "b"), all)).isTrue();
    assertThat(matches("a,a", ",", List.of("a", "b"), all)).isFalse();
    assertThat(matches("a, b", ",", List.of("b"), any)).isFalse();
    assertThat(matches("a, b", ",", List.of(" b"), any)).isTrue();
    assertThat(matches("A", ",", List.of("a"), any)).isFalse();
    assertThat(matches("a.*b.*c", ".*", List.of("b"), any)).isTrue();
    assertThat(matches("a||b||c", "||", List.of("a", "c"), all)).isTrue();
    assertThat(matches(",a,,", ",", List.of("a"), all)).isTrue();
    assertThat(matches("", ",", List.of("a"), any)).isFalse();
    assertThat(matches(null, ",", List.of("a"), any)).isFalse();
    assertThat(matches(null, ",", List.of("a"), all)).isFalse();
    assertThat(matches("नमस्ते,world", ",", List.of("नमस्ते"), any)).isTrue();
  }

  @Test
  void copiesConfigAndPreservesSchemaOriginalRowAndEveryNativeKind() throws Exception {
    var input = table();
    var tokens = new ArrayList<>(List.of("a"));
    try (var transform =
        new FilterTokenTransform(input, "tags", ",", tokens, FilterTokenTransform.MatchMode.ALL)) {
      tokens.set(0, "changed");
      assertThat(transform.producedTable()).isSameAs(input);
      transform.open();
      for (var kind : RowKind.values()) {
        var row = SeaTunnelRow.ofKind(kind, 1L, "a");
        assertThat(transform.apply(row)).containsExactly(row);
        assertThat(transform.apply(row).get(0)).isSameAs(row);
        assertThat(row.getRowKind()).isEqualTo(kind);
      }
    }
  }

  @Test
  void validatesConfigAndBoundariesBeforeOpening() throws Exception {
    var any = FilterTokenTransform.MatchMode.ANY;
    for (String delimiter : List.of("", "x".repeat(129)))
      assertThatThrownBy(
              () -> new FilterTokenTransform(table(), "tags", delimiter, List.of("a"), any))
          .isInstanceOf(IllegalArgumentException.class);
    for (List<String> tokens :
        List.of(
            List.<String>of(),
            List.of(""),
            List.of("a", "a"),
            List.of("x".repeat(4097)),
            Collections.nCopies(129, "a"),
            Arrays.asList("a", null)))
      assertThatThrownBy(() -> new FilterTokenTransform(table(), "tags", ",", tokens, any))
          .isInstanceOf(IllegalArgumentException.class);
    for (String field : List.of("id", "missing"))
      assertThatThrownBy(() -> new FilterTokenTransform(table(), field, ",", List.of("a"), any))
          .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FilterTokenTransform(table(), "tags", null, List.of("a"), any))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FilterTokenTransform(table(), "tags", ",", null, any))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new FilterTokenTransform(table(), "tags", ",", List.of("a"), null))
        .isInstanceOf(IllegalArgumentException.class);
    var tokens = IntStream.range(0, 128).mapToObj(i -> "t" + i).toList();
    assertThat(matches(String.join(",", tokens), ",", tokens, FilterTokenTransform.MatchMode.ALL))
        .isTrue();
    assertThat(matches("x".repeat(4096), ",", List.of("x".repeat(4096)), any)).isTrue();
    assertThat(matches("a" + "|".repeat(128) + "b", "|".repeat(128), List.of("b"), any)).isTrue();
  }

  @Test
  void validatesRuntimeAndLifecycleAndChecksSizeBeforeEarlyReturn() throws Exception {
    var transform =
        new FilterTokenTransform(
            table(), "tags", ",", List.of("a"), FilterTokenTransform.MatchMode.ANY);
    var row = new SeaTunnelRow(1L, "a");
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    transform.open();
    try {
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(() -> transform.apply(null)).isInstanceOf(NullPointerException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, 1)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  transform.apply(
                      new SeaTunnelRow(
                          1L, "a," + "x".repeat(FilterTokenTransform.MAX_INPUT_CHARS))))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("limit");
      assertThat(transform.apply(row)).containsExactly(row);
    } finally {
      transform.close();
    }
    transform.close();
    assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
    var unused =
        new FilterTokenTransform(
            table(), "tags", ",", List.of("a"), FilterTokenTransform.MatchMode.ANY);
    unused.close();
    unused.close();
    assertThatThrownBy(unused::open).isInstanceOf(IllegalStateException.class);
  }

  @Test
  void validatesInstalledPluginOptionsAndMode() {
    var plugin = new FilterTokenPlugin();
    assertThat(plugin.identifier()).isEqualTo("filter-token");
    for (Map<String, Object> options :
        List.of(
            Map.<String, Object>of(),
            Map.<String, Object>of(
                "field", "tags", "delimiter", ",", "tokens", List.of(1), "mode", "any"),
            Map.<String, Object>of(
                "field", "tags", "delimiter", ",", "tokens", List.of("a"), "mode", "ANY"),
            Map.<String, Object>of(
                "field",
                "tags",
                "delimiter",
                ",",
                "tokens",
                List.of("a"),
                "mode",
                "any",
                "extra",
                "x")))
      assertThatThrownBy(() -> plugin.createTransform(options, table()))
          .isInstanceOf(IllegalArgumentException.class);
  }
}
