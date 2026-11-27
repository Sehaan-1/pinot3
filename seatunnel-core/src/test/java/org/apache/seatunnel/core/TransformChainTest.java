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
package org.apache.seatunnel.core;

import static org.assertj.core.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.junit.jupiter.api.Test;

class TransformChainTest {
  private static final CatalogTable TABLE =
      new CatalogTable(
          "one", List.of(new CatalogTable.Column("n", CatalogTable.DataType.INT, false)));

  @Test
  void propagatesFanoutAndFilteringInOrderWithoutBufferingInput() throws Exception {
    var output = new ArrayList<SeaTunnelRow>();
    var lifecycle = new ArrayList<String>();
    try (var chain = new TransformChain()) {
      chain.add(
          new Operator("first", lifecycle) {
            public List<SeaTunnelRow> apply(SeaTunnelRow row) {
              return List.of(row, new SeaTunnelRow(2));
            }
          });
      chain.add(
          new Operator("filter", lifecycle) {
            public List<SeaTunnelRow> apply(SeaTunnelRow row) {
              return row.getField(0).equals(1) ? List.of() : List.of(row);
            }
          });
      chain.open();
      chain.emit(new SeaTunnelRow(1), output::add);
    }
    assertThat(output).containsExactly(new SeaTunnelRow(2));
    assertThat(lifecycle)
        .containsExactly("open-first", "open-filter", "close-filter", "close-first");
  }

  @Test
  void closesAllOperatorsAfterPartialStartupAndSuppressesCloseFailures() {
    var lifecycle = new ArrayList<String>();
    assertThatThrownBy(
            () -> {
              try (var chain = new TransformChain()) {
                chain.add(
                    new Operator("first", lifecycle) {
                      public void close() throws Exception {
                        super.close();
                        throw new Exception("close-first");
                      }
                    });
                chain.add(
                    new Operator("second", lifecycle) {
                      public void open() throws Exception {
                        super.open();
                        throw new Exception("open-failed");
                      }

                      public void close() throws Exception {
                        super.close();
                        throw new Exception("close-second");
                      }
                    });
                chain.open();
              }
            })
        .hasMessage("open-failed")
        .satisfies(
            failure -> {
              assertThat(failure.getSuppressed()).hasSize(1);
              assertThat(failure.getSuppressed()[0]).hasMessage("close-second");
              assertThat(failure.getSuppressed()[0].getSuppressed()).hasSize(1);
            });
    assertThat(lifecycle)
        .containsExactly("open-first", "open-second", "close-second", "close-first");
  }

  @Test
  void rejectsMalformedStageOutputsAndPropagatesSinkFailure() throws Exception {
    for (int mode : List.of(0, 1, 2)) {
      try (var chain = new TransformChain()) {
        chain.add(
            new Operator("bad", new ArrayList<>()) {
              public List<SeaTunnelRow> apply(SeaTunnelRow row) {
                return mode == 0
                    ? null
                    : mode == 1
                        ? java.util.Arrays.asList((SeaTunnelRow) null)
                        : List.of(new SeaTunnelRow(1, 2));
              }
            });
        chain.open();
        assertThatThrownBy(() -> chain.emit(new SeaTunnelRow(1), row -> {}))
            .isInstanceOf(RuntimeException.class);
      }
    }
    try (var chain = new TransformChain()) {
      assertThatThrownBy(
              () ->
                  chain.emit(
                      new SeaTunnelRow(1),
                      row -> {
                        throw new Exception("sink-failed");
                      }))
          .hasMessage("sink-failed");
    }
  }

  @Test
  void closesRegisteredOperatorsWhenLaterSchemaConstructionFails() {
    var lifecycle = new ArrayList<String>();
    assertThatThrownBy(
            () -> {
              try (var chain = new TransformChain()) {
                chain.add(new Operator("first", lifecycle));
                chain.add(
                    new Operator("broken", lifecycle) {
                      public CatalogTable producedTable() {
                        throw new IllegalArgumentException("schema-failed");
                      }
                    });
              }
            })
        .hasMessage("schema-failed");
    assertThat(lifecycle).containsExactly("close-broken", "close-first");
  }

  private static class Operator implements SeaTunnelTransform {
    final String id;
    final List<String> lifecycle;

    Operator(String id, List<String> lifecycle) {
      this.id = id;
      this.lifecycle = lifecycle;
    }

    public CatalogTable producedTable() {
      return TABLE;
    }

    public void open() throws Exception {
      lifecycle.add("open-" + id);
    }

    public List<SeaTunnelRow> apply(SeaTunnelRow row) {
      return List.of(row);
    }

    public void close() throws Exception {
      lifecycle.add("close-" + id);
    }
  }
}
