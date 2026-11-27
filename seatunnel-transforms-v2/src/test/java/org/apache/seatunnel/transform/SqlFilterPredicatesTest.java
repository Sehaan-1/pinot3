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

import java.util.Collections;
import java.util.List;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.junit.jupiter.api.Test;

class SqlFilterPredicatesTest {
  private CatalogTable table() {
    return new CatalogTable(
        "events",
        List.of(new Column("id", DataType.LONG, true), new Column("name", DataType.STRING, true)));
  }

  private boolean matches(String condition, Long id, String name) throws Exception {
    try (var transform = new SqlFilterTransform(table(), condition)) {
      assertThat(transform.producedTable()).isEqualTo(table());
      transform.open();
      var row = new SeaTunnelRow(id, name);
      var result = transform.apply(row);
      if (!result.isEmpty()) assertThat(result.get(0)).isSameAs(row);
      return !result.isEmpty();
    }
  }

  @Test
  void evaluatesInNotInAndUnknownWithoutJavaMembershipSemantics() throws Exception {
    assertThat(matches("id IN (1,2,3)", 2L, null)).isTrue();
    assertThat(matches("id IN (1,2,3)", 4L, null)).isFalse();
    assertThat(matches("id NOT IN (1,2,3)", 4L, null)).isTrue();
    assertThat(matches("id NOT IN (1,2,NULL)", 4L, null)).isFalse();
    assertThat(matches("id IN (1,NULL,2)", 2L, null)).isTrue();
    assertThat(matches("id IN (1,NULL,2)", 4L, null)).isFalse();
    assertThat(matches("id IN (1,2)", null, null)).isFalse();
    assertThat(matches("name IN ('Ada','O''Reilly')", 1L, "O'Reilly")).isTrue();
    assertThat(matches("id IN (id,1,1)", 4L, null)).isTrue();
    assertThat(matches("id IN (1)", 1L, null)).isTrue();
  }

  @Test
  void evaluatesInclusiveBetweenNotBetweenReversedAndNullEndpoints() throws Exception {
    assertThat(matches("id BETWEEN 1 AND 3", 1L, null)).isTrue();
    assertThat(matches("id BETWEEN 1 AND 3", 3L, null)).isTrue();
    assertThat(matches("id BETWEEN 3 AND 1", 2L, null)).isFalse();
    assertThat(matches("id NOT BETWEEN 1 AND 3", 4L, null)).isTrue();
    assertThat(matches("id BETWEEN NULL AND 3", 2L, null)).isFalse();
    assertThat(matches("id NOT BETWEEN NULL AND 3", 2L, null)).isFalse();
    assertThat(matches("id BETWEEN id AND id", 4L, null)).isTrue();
    assertThat(matches("NOT (id IN (1,NULL)) OR id BETWEEN 3 AND 5", 4L, null)).isTrue();
  }

  @Test
  void rejectsNonFlatListsSubqueriesDecorationsAndMalformedInput() {
    for (String condition :
        List.of(
            "id IN ()",
            "id IN (SELECT id)",
            "id IN ((1,2),(3,4))",
            "id IN (LENGTH(name))",
            "id GLOBAL IN (1)",
            "id(+) IN (1)",
            "id BETWEEN (SELECT id) AND 2",
            "id BETWEEN 1+1 AND 3",
            "id BETWEEN SYMMETRIC 1 AND 3",
            "id IN (unknown)",
            "id IN (1); SELECT id",
            "id IN ('unclosed)",
            "id[1] IN (1)",
            "id COMMENT 'x' = 1"))
      assertThatThrownBy(() -> new SqlFilterTransform(table(), condition))
          .as(condition)
          .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void accepts64ItemsButRejects65BeforeOpening() throws Exception {
    String list = String.join(",", Collections.nCopies(64, "1"));
    assertThat(matches("id IN (" + list + ")", 1L, null)).isTrue();
    assertThatThrownBy(() -> new SqlFilterTransform(table(), "id IN (" + list + ",1)"))
        .hasMessageContaining("1..64");
  }
}
