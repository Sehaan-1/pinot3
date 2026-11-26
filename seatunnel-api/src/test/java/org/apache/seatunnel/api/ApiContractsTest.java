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
package org.apache.seatunnel.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.common.exception.ConfigException;
import org.junit.jupiter.api.Test;

class ApiContractsTest {
  @Test
  void rowOwnsItsArrayAndHasValueSemantics() {
    Object[] fields = {1, "value", null};
    SeaTunnelRow row = new SeaTunnelRow(fields);
    fields[0] = 99;
    row.getFields()[1] = "changed";
    assertThat(row.getArity()).isEqualTo(3);
    assertThat(row.getField(0)).isEqualTo(1);
    assertThat(row.getField(1)).isEqualTo("value");
    assertThat(row).isEqualTo(new SeaTunnelRow(1, "value", null));
    assertThat(row.hashCode()).isEqualTo(new SeaTunnelRow(1, "value", null).hashCode());
    assertThat(row.toString()).isEqualTo("[1, value, null]");
    assertThatThrownBy(() -> row.getField(3)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> new SeaTunnelRow((Object[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void rowsAndSchemasRoundTripThroughSerialization() throws Exception {
    SeaTunnelRow row = new SeaTunnelRow(new byte[] {1, 2}, "hello");
    assertThat(roundTrip(row)).isEqualTo(row);
    CatalogTable table = new CatalogTable("events", List.of(new Column("id", DataType.LONG, false)));
    assertThat(roundTrip(table)).isEqualTo(table);
  }

  private Object roundTrip(Object value) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return in.readObject();
    }
  }

  @Test
  void schemaIsOrderedImmutableAndRejectsInvalidNames() {
    Column id = new Column("id", DataType.LONG, false);
    List<Column> columns = new ArrayList<>(List.of(id));
    CatalogTable table = new CatalogTable("events", columns);
    columns.clear();
    assertThat(table.columns()).containsExactly(id);
    assertThatThrownBy(() -> table.columns().clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> new CatalogTable("events", List.of(id, id)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CatalogTable(" ", List.of(id)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new CatalogTable("events", List.of()))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Column("", DataType.INT, true))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new Column("id", null, true))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void optionsAreTypedAndDefensivelyCopied() {
    Map<String, Object> values = new HashMap<>(Map.of("parallelism", 4));
    Options options = new Options(values);
    values.put("parallelism", 8);
    assertThat(options.get(Option.required("parallelism", Integer.class))).isEqualTo(4);
    assertThat(options.get(Option.withDefault("mode", String.class, "batch"))).isEqualTo("batch");
    assertThatThrownBy(() -> options.get(Option.required("missing", String.class)))
        .isInstanceOf(ConfigException.class).hasMessageContaining("missing");
    assertThatThrownBy(() -> options.get(Option.required("parallelism", String.class)))
        .isInstanceOf(ConfigException.class).hasMessageContaining("parallelism");
    assertThatThrownBy(() -> Option.required(" ", String.class))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> Option.withDefault("mode", String.class, null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void apiHasNoEngineOrConnectorImports() throws Exception {
    try (var paths = Files.walk(Path.of("src/main/java"))) {
      for (Path path : paths.filter(p -> p.toString().endsWith(".java")).toList()) {
        String source = Files.readString(path);
        assertThat(source).doesNotContain("org.apache.seatunnel.engine", "org.apache.flink",
            "org.apache.spark", "org.apache.seatunnel.connectors");
      }
    }
  }
}
