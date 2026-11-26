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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.graph.ElementOrder;
import com.google.common.graph.ImmutableValueGraph;
import com.google.common.graph.MutableValueGraph;
import com.google.common.graph.Traverser;
import com.google.common.graph.ValueGraphBuilder;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import org.apache.seatunnel.engine.dag.LogicalEdge.Exchange;
import org.apache.seatunnel.engine.dag.LogicalNode.Kind;

/** Validated immutable topology. Construction order does not influence traversal or planning. */
public final class LogicalDag {
  private final ImmutableSortedMap<String, LogicalNode> nodes;
  private final ImmutableList<LogicalEdge> edges;
  private final ImmutableValueGraph<String, Exchange> graph;
  private final ImmutableList<LogicalNode> topologicalOrder;

  public LogicalDag(Collection<LogicalNode> nodes, Collection<LogicalEdge> edges) {
    Map<String, LogicalNode> indexed = new TreeMap<>();
    for (LogicalNode node : nodes) {
      Objects.requireNonNull(node, "node");
      if (indexed.putIfAbsent(node.id(), node) != null) {
        throw new PlanValidationException("Duplicate operator ID: " + node.id());
      }
    }
    if (indexed.isEmpty()) {
      throw new PlanValidationException("DAG must contain at least one complete pipeline");
    }
    this.nodes = ImmutableSortedMap.copyOf(indexed);
    this.edges =
        ImmutableList.copyOf(edges).stream()
            .sorted(
                Comparator.comparing(LogicalEdge::sourceId).thenComparing(LogicalEdge::targetId))
            .collect(ImmutableList.toImmutableList());
    MutableValueGraph<String, Exchange> mutable =
        ValueGraphBuilder.directed()
            .allowsSelfLoops(false)
            .nodeOrder(ElementOrder.<String>natural())
            .incidentEdgeOrder(ElementOrder.stable())
            .build();
    this.nodes.keySet().forEach(mutable::addNode);
    for (LogicalEdge edge : this.edges) {
      LogicalNode source = node(edge.sourceId());
      LogicalNode target = node(edge.targetId());
      if (mutable.hasEdgeConnecting(source.id(), target.id())) {
        throw new PlanValidationException("Duplicate edge: " + source.id() + " -> " + target.id());
      }
      if (source.kind() == Kind.SINK || target.kind() == Kind.SOURCE) {
        throw new PlanValidationException(
            "Invalid operator topology for edge: " + source.id() + " -> " + target.id());
      }
      if (!source.outputTable().columns().equals(target.inputTable().columns())) {
        throw new PlanValidationException(
            "Incompatible edge schemas: " + source.id() + " -> " + target.id());
      }
      if (edge.exchange() == Exchange.FORWARD && source.parallelism() != target.parallelism()) {
        throw new PlanValidationException(
            "FORWARD requires equal parallelism: " + source.id() + " -> " + target.id());
      }
      mutable.putEdgeValue(source.id(), target.id(), edge.exchange());
    }
    graph = ImmutableValueGraph.copyOf(mutable);
    List<String> order =
        ImmutableList.copyOf(Traverser.forGraph(graph.asGraph()).depthFirstPostOrder(graph.nodes()))
            .reverse();
    Map<String, Integer> positions = new HashMap<>();
    for (int i = 0; i < order.size(); i++) {
      positions.put(order.get(i), i);
    }
    for (LogicalEdge edge : this.edges) {
      if (positions.get(edge.sourceId()) >= positions.get(edge.targetId())) {
        throw new PlanValidationException(
            "Cycle detected at edge: " + edge.sourceId() + " -> " + edge.targetId());
      }
    }
    for (LogicalNode node : this.nodes.values()) {
      int inputs = graph.inDegree(node.id());
      int outputs = graph.outDegree(node.id());
      boolean valid =
          switch (node.kind()) {
            case SOURCE -> inputs == 0 && outputs > 0;
            case TRANSFORM -> inputs == 1 && outputs > 0;
            case SINK -> inputs == 1 && outputs == 0;
          };
      if (!valid) {
        throw new PlanValidationException(
            "Invalid " + node.kind() + " topology for operator: " + node.id());
      }
    }
    topologicalOrder = order.stream().map(this::node).collect(ImmutableList.toImmutableList());
  }

  public LogicalNode node(String id) {
    LogicalNode node = nodes.get(Objects.requireNonNull(id, "id"));
    if (node == null) {
      throw new PlanValidationException("Unknown operator: " + id);
    }
    return node;
  }

  /** Nodes sorted by operator ID, independent of topological order. */
  public List<LogicalNode> nodes() {
    return nodes.values().asList();
  }

  public List<LogicalEdge> edges() {
    return edges;
  }

  public List<LogicalNode> topologicalOrder() {
    return topologicalOrder;
  }

  public Exchange exchange(String sourceId, String targetId) {
    node(sourceId);
    node(targetId);
    return graph
        .edgeValue(sourceId, targetId)
        .orElseThrow(() -> new PlanValidationException("No edge: " + sourceId + " -> " + targetId));
  }
}
