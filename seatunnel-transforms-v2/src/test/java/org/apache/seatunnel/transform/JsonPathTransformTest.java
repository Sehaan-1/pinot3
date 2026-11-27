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

class JsonPathTransformTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(
            new CatalogTable.Column("id", CatalogTable.DataType.LONG, false),
            new CatalogTable.Column("json", CatalogTable.DataType.STRING, true)));
  }

  private Object extract(String path, String json) throws Exception {
    try (var transform = new JsonPathTransform(table(), "json", path, "extracted")) {
      transform.open();
      return transform.apply(new SeaTunnelRow(1L, json)).get(0).getField(2);
    }
  }

  @Test
  void extractsStringsScalarsStructuresAndIndefiniteArrays() throws Exception {
    String json =
        "{\"user\":{\"name\":\"Ada\"},\"num\":4,\"flag\":true,\"none\":null,\"items\":[{\"name\":\"a\"},{\"name\":\"b\"}]}";
    assertThat(extract("$.user.name", json)).isEqualTo("Ada");
    assertThat(extract("$.num", json)).isEqualTo("4");
    assertThat(extract("$.flag", json)).isEqualTo("true");
    assertThat(extract("$.user", json)).isEqualTo("{\"name\":\"Ada\"}");
    assertThat(extract("$.items[*].name", json)).isEqualTo("[\"a\",\"b\"]");
    assertThat(extract("$.items[?(@.name == 'b')].name", json)).isEqualTo("[\"b\"]");
    assertThat(extract("$.items[?(@.name == 'z')].name", json)).isEqualTo("[]");
    assertThat(extract("$.missing.leaf", json)).isNull();
    assertThat(extract("$.none", json)).isNull();
    assertThat(extract("$", null)).isNull();
    assertThat(extract("$", "null")).isNull();
  }

  @Test
  void rejectsInvalidJsonDuplicateKeysTrailingTokensExcessiveNestingAndSize() {
    for (String json :
        List.of(
            "not-json", "{", "{\"x\":1,\"x\":2}", "{} {}", "[".repeat(65) + "0" + "]".repeat(65)))
      assertThatThrownBy(() -> extract("$.missing", json)).hasMessage("Invalid JSON input");
    assertThatThrownBy(() -> extract("$", " ".repeat(JsonPathTransform.MAX_INPUT_CHARS + 1)))
        .hasMessage("JSON input exceeds character limit");
  }

  @Test
  void validatesSchemaOptionsAndLifecycleWithoutMutation() throws Exception {
    assertThatThrownBy(() -> new JsonPathTransform(table(), "id", "$", "out"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JsonPathTransform(table(), "missing", "$", "out"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JsonPathTransform(table(), "json", "$", "id"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JsonPathTransform(table(), "json", "$", " "))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JsonPathTransform(table(), "json", " ", "out"))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new JsonPathTransform(table(), "json", "$[", "out"))
        .isInstanceOf(RuntimeException.class);
    assertThatThrownBy(
            () ->
                new JsonPathPlugin()
                    .createTransform(Map.of("field", "json", "path", 1, "target", "out"), table()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(
            () ->
                new JsonPathPlugin()
                    .createTransform(
                        Map.of("field", "json", "path", "$", "target", "out", "extra", true),
                        table()))
        .isInstanceOf(IllegalArgumentException.class);
    try (var transform = new JsonPathTransform(table(), "json", "$.name", "out")) {
      var row = new SeaTunnelRow(1L, "{\"name\":\"Ada\"}");
      assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
      transform.open();
      assertThat(transform.producedTable().columns())
          .containsExactly(
              table().columns().get(0),
              table().columns().get(1),
              new CatalogTable.Column("out", CatalogTable.DataType.STRING, true));
      assertThat(transform.apply(row))
          .containsExactly(new SeaTunnelRow(1L, "{\"name\":\"Ada\"}", "Ada"));
      assertThat(row.getArity()).isEqualTo(2);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(() -> transform.apply(new SeaTunnelRow(1L, 3)))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
      transform.close();
      transform.close();
      assertThatThrownBy(() -> transform.apply(row)).isInstanceOf(IllegalStateException.class);
      assertThatThrownBy(transform::open).isInstanceOf(IllegalStateException.class);
    }
  }
}
