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
package org.apache.seatunnel.engine.dag;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.CatalogTable.Column;
import org.apache.seatunnel.api.table.CatalogTable.DataType;
import org.apache.seatunnel.engine.dag.LogicalEdge.Exchange;
import org.apache.seatunnel.engine.dag.LogicalNode.Kind;
import org.junit.jupiter.api.Test;

class LogicalDagTest {
  private static final CatalogTable TABLE =
      new CatalogTable("events", List.of(new Column("id", DataType.LONG, false)));

  private LogicalNode node(String id, Kind kind) {
    return new LogicalNode(id, kind, "test-plugin", 2, TABLE);
  }

  private LogicalEdge edge(String source, String target) {
    return new LogicalEdge(source, target, Exchange.FORWARD);
  }

  @Test
  void validatesCompleteChainAndSchemaNamesNeedNotMatch() {
    LogicalNode sink =
        new LogicalNode(
            "sink", Kind.SINK, "console", 2, new CatalogTable("renamed", TABLE.columns()));
    LogicalDag dag =
        new LogicalDag(
            List.of(node("source", Kind.SOURCE), node("transform", Kind.TRANSFORM), sink),
            List.of(edge("source", "transform"), edge("transform", "sink")));
    assertThat(dag.topologicalOrder())
        .extracting(LogicalNode::id)
        .containsExactly("source", "transform", "sink");
    assertThat(dag.exchange("source", "transform")).isEqualTo(Exchange.FORWARD);
    assertThat(dag.node("sink")).isEqualTo(sink);
  }

  @Test
  void acceptsExplicitSchemaChangesButRejectsInvalidDirectionalSchemaMetadata() {
    CatalogTable output =
        new CatalogTable("names", List.of(new Column("name", DataType.STRING, false)));
    LogicalNode transform = new LogicalNode("x", Kind.TRANSFORM, "rename", 2, TABLE, output);
    LogicalNode sink = new LogicalNode("t", Kind.SINK, "console", 2, output);
    LogicalDag dag =
        new LogicalDag(
            List.of(node("s", Kind.SOURCE), transform, sink),
            List.of(edge("s", "x"), edge("x", "t")));
    assertThat(dag.node("x").inputTable()).isEqualTo(TABLE);
    assertThat(dag.node("x").outputTable()).isEqualTo(output);
    assertThatThrownBy(() -> new LogicalNode("s", Kind.SOURCE, "fake", 1, TABLE, TABLE))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalNode("x", Kind.TRANSFORM, "rename", 1, null, TABLE))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalNode("t", Kind.SINK, "console", 1, TABLE, TABLE))
        .isInstanceOf(PlanValidationException.class);
  }

  @Test
  void branchingAndIndependentPipelinesHaveDeterministicDependencyOrder() {
    List<LogicalNode> nodes =
        new ArrayList<>(
            List.of(
                node("s", Kind.SOURCE),
                node("a", Kind.TRANSFORM),
                node("b", Kind.SINK),
                node("c", Kind.SINK),
                node("d", Kind.SOURCE),
                node("e", Kind.SINK)));
    List<LogicalEdge> edges =
        new ArrayList<>(List.of(edge("s", "a"), edge("s", "b"), edge("a", "c"), edge("d", "e")));
    List<LogicalNode> expected = new LogicalDag(nodes, edges).topologicalOrder();
    Random random = new Random(123);
    for (int i = 0; i < 30; i++) {
      Collections.shuffle(nodes, random);
      Collections.shuffle(edges, random);
      LogicalDag dag = new LogicalDag(nodes, edges);
      assertThat(dag.topologicalOrder()).isEqualTo(expected);
      for (LogicalEdge edge : dag.edges()) {
        assertThat(expected.indexOf(dag.node(edge.sourceId())))
            .isLessThan(expected.indexOf(dag.node(edge.targetId())));
      }
    }
  }

  @Test
  void snapshotsCollectionsAndExposesOnlyImmutableViews() {
    List<LogicalNode> nodes =
        new ArrayList<>(List.of(node("s", Kind.SOURCE), node("t", Kind.SINK)));
    List<LogicalEdge> edges = new ArrayList<>(List.of(edge("s", "t")));
    LogicalDag dag = new LogicalDag(nodes, edges);
    nodes.clear();
    edges.clear();
    assertThat(dag.nodes()).hasSize(2);
    assertThat(dag.edges()).hasSize(1);
    assertThatThrownBy(() -> dag.nodes().clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> dag.edges().clear()).isInstanceOf(UnsupportedOperationException.class);
    assertThatThrownBy(() -> dag.topologicalOrder().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void rejectsEmptyDuplicateAndUnknownOperatorsAndDuplicateEdges() {
    List<LogicalNode> nodes = List.of(node("s", Kind.SOURCE), node("t", Kind.SINK));
    assertThatThrownBy(() -> new LogicalDag(List.of(), List.of()))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("complete pipeline");
    assertThatThrownBy(() -> new LogicalDag(List.of(nodes.get(0), nodes.get(0)), List.of()))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Duplicate operator");
    assertThatThrownBy(() -> new LogicalDag(nodes, List.of(edge("s", "unknown"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Unknown operator");
    assertThatThrownBy(() -> new LogicalDag(nodes, List.of(edge("s", "t"), edge("s", "t"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Duplicate edge");
    LogicalDag dag = new LogicalDag(nodes, List.of(edge("s", "t")));
    assertThatThrownBy(() -> dag.node("absent")).isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> dag.exchange("t", "s"))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("No edge");
  }

  @Test
  void rejectsSelfEdgesAndCyclesIncludingInDisconnectedComponents() {
    assertThatThrownBy(() -> edge("a", "a"))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Self-edge");
    List<LogicalNode> nodes =
        List.of(
            node("s", Kind.SOURCE),
            node("t", Kind.SINK),
            node("a", Kind.TRANSFORM),
            node("b", Kind.TRANSFORM),
            node("c", Kind.TRANSFORM));
    assertThatThrownBy(
            () ->
                new LogicalDag(
                    nodes, List.of(edge("s", "t"), edge("a", "b"), edge("b", "c"), edge("c", "a"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Cycle detected");
  }

  @Test
  void rejectsDanglingAndReversedOperatorShapesAndUnsupportedFanIn() {
    assertThatThrownBy(() -> new LogicalDag(List.of(node("s", Kind.SOURCE)), List.of()))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("topology");
    assertThatThrownBy(
            () ->
                new LogicalDag(
                    List.of(node("s", Kind.SOURCE), node("t", Kind.SINK)), List.of(edge("t", "s"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("topology");
    assertThatThrownBy(
            () ->
                new LogicalDag(
                    List.of(node("s", Kind.SOURCE), node("u", Kind.SOURCE), node("t", Kind.SINK)),
                    List.of(edge("s", "t"), edge("u", "t"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("topology");
    assertThatThrownBy(
            () ->
                new LogicalDag(
                    List.of(node("s", Kind.SOURCE), node("x", Kind.TRANSFORM)),
                    List.of(edge("s", "x"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("topology");
  }

  @Test
  void rejectsIncompatibleSchemasAndForwardParallelismButAllowsShuffle() {
    LogicalNode source = node("s", Kind.SOURCE);
    LogicalNode sink = new LogicalNode("t", Kind.SINK, "console", 3, TABLE);
    assertThatThrownBy(() -> new LogicalDag(List.of(source, sink), List.of(edge("s", "t"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("equal parallelism");
    assertThat(
            new LogicalDag(
                    List.of(source, sink), List.of(new LogicalEdge("s", "t", Exchange.SHUFFLE)))
                .nodes())
        .hasSize(2);
    LogicalNode different =
        new LogicalNode(
            "t",
            Kind.SINK,
            "console",
            2,
            new CatalogTable("events", List.of(new Column("id", DataType.LONG, true))));
    assertThatThrownBy(() -> new LogicalDag(List.of(source, different), List.of(edge("s", "t"))))
        .isInstanceOf(PlanValidationException.class)
        .hasMessageContaining("Incompatible edge schemas");
  }

  @Test
  void rejectsInvalidNodeAndEdgeMetadata() {
    assertThatThrownBy(() -> node(" ", Kind.SOURCE)).isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalNode("s", Kind.SOURCE, "INVALID", 1, TABLE))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalNode("s", Kind.SOURCE, "fake", 0, TABLE))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalEdge("s", " ", Exchange.SHUFFLE))
        .isInstanceOf(PlanValidationException.class);
    assertThatThrownBy(() -> new LogicalEdge("s", "t", null))
        .isInstanceOf(NullPointerException.class);
  }

  @Test
  void handlesDeepGraphsWithoutRecursiveTraversal() {
    int size = 10_000;
    List<LogicalNode> nodes = new ArrayList<>();
    List<LogicalEdge> edges = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      Kind kind = i == 0 ? Kind.SOURCE : (i == size - 1 ? Kind.SINK : Kind.TRANSFORM);
      nodes.add(node("node-" + i, kind));
      if (i > 0) {
        edges.add(edge("node-" + (i - 1), "node-" + i));
      }
    }
    LogicalDag dag = new LogicalDag(nodes, edges);
    assertThat(dag.topologicalOrder()).isEqualTo(nodes);
  }
}
