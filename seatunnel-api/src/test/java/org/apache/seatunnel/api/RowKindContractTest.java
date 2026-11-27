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

import static org.assertj.core.api.Assertions.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import org.apache.seatunnel.api.table.RowKind;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class RowKindContractTest {
  @Test
  void keepsDefaultInsertVarargsDataOwnershipAndHashCompatibility() {
    Object[] fields = {1, "a"};
    var row = new SeaTunnelRow(fields);
    fields[0] = 2;
    assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
    assertThat(row.getFields()).containsExactly(1, "a");
    assertThat(row.hashCode()).isEqualTo(Arrays.deepHashCode(new Object[] {1, "a"}));
    assertThat(row.toString()).isEqualTo("[1, a]");
    assertThat(new SeaTunnelRow(RowKind.DELETE).getRowKind()).isEqualTo(RowKind.INSERT);
    assertThat(new SeaTunnelRow(RowKind.DELETE).getField(0)).isEqualTo(RowKind.DELETE);
    assertThat(new SeaTunnelRow(RowKind.DELETE, new Object[] {1}).getArity()).isEqualTo(2);
  }

  @Test
  void copiesFieldsAndKindIndependentlyAndIncludesKindInEquality() {
    assertThat(Arrays.stream(RowKind.values()).map(RowKind::shortString))
        .containsExactly("+I", "-U", "+U", "-D");
    for (var kind : RowKind.values()) {
      byte[] mutable = {1};
      Object[] fields = {mutable, "a"};
      var row = SeaTunnelRow.ofKind(kind, fields);
      fields[1] = "changed";
      assertThat(row.getFields()).containsExactly(mutable, "a");
      var exposed = row.getFields();
      exposed[1] = "changed";
      assertThat(row.getField(1)).isEqualTo("a");
      assertThat(row.getField(0)).isSameAs(mutable);
      assertThat(row.withFields(2).getRowKind()).isEqualTo(kind);
      assertThat(row.withKind(RowKind.INSERT)).isEqualTo(new SeaTunnelRow(mutable, "a"));
      assertThat(row).isEqualTo(SeaTunnelRow.ofKind(kind, new byte[] {1}, "a"));
      assertThat(row.hashCode())
          .isEqualTo(SeaTunnelRow.ofKind(kind, new byte[] {1}, "a").hashCode());
    }
    assertThat(SeaTunnelRow.ofKind(RowKind.DELETE, 1)).isNotEqualTo(new SeaTunnelRow(1));
    assertThatThrownBy(() -> SeaTunnelRow.ofKind(null, 1)).isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new SeaTunnelRow(1).withKind(null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> SeaTunnelRow.ofKind(RowKind.INSERT, (Object[]) null))
        .isInstanceOf(NullPointerException.class);
    assertThatThrownBy(() -> new SeaTunnelRow(1).withFields((Object[]) null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void readsPreMetadataLegacyStreamAsInsertAndRoundTripsAllKinds() throws Exception {
    try (var resource = getClass().getResourceAsStream("/legacy-insert-row.base64")) {
      assertThat(resource).isNotNull();
      byte[] bytes =
          Base64.getDecoder()
              .decode(new String(resource.readAllBytes(), StandardCharsets.US_ASCII).trim());
      try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
        var row = (SeaTunnelRow) input.readObject();
        assertThat(row.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(row).isEqualTo(new SeaTunnelRow(7L, "legacy"));
        assertThat(row.hashCode()).isEqualTo(new SeaTunnelRow(7L, "legacy").hashCode());
      }
    }
    for (var kind : RowKind.values()) {
      var row = SeaTunnelRow.ofKind(kind, 1L, "a", null);
      var bytes = new ByteArrayOutputStream();
      try (var output = new ObjectOutputStream(bytes)) {
        output.writeObject(row);
      }
      try (var input = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
        assertThat(input.readObject()).isEqualTo(row);
      }
    }
  }
}
