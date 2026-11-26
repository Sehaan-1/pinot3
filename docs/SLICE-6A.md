# Slice 6a — Logical DAG and physical planning

## Specification and dependency plan

PR title: `[Slice 6a] Validated logical DAG and deterministic physical task groups`.

Add `seatunnel-engine`, packages `org.apache.seatunnel.engine.dag` and `.plan`.
Production dependencies: `org.apache.seatunnel:seatunnel-api:3.0.0-SNAPSHOT` and
`com.google.guava:guava:33.2.1-jre` (already root-managed). Engine depends on API,
never the reverse. No core, CLI, plugin, connector, Hazelcast or Netty dependency.
Tests adopt existing JUnit 5.11.0 and AssertJ 3.26.3.

Build vs adopt: use Guava's directed value graph, immutable graph snapshots and
iterative `Traverser.forGraph(...).depthFirstPostOrder(...)`. Project code validates
operator semantics and verifies that every edge points forward in the reversed
postorder. A backwards edge proves a cycle; no custom DFS, recursive cycle finder,
graph container, scheduler or partitioning algorithm is introduced.

## Logical contracts

- Immutable nodes contain a nonblank unique ID, SOURCE/TRANSFORM/SINK kind, plugin
  identifier, positive parallelism, and input/output CatalogTables. IDs bind future runtime
  operator configuration; plans contain no live operators, credentials or loaders.
- Sources require an output schema and no input schema; sinks require an input
  schema and no output schema; transforms require both. Adjacent output/input
  column lists must match exactly (including nullability); table display names
  need not match. A transform may explicitly change schema between its input and
  output. No implicit coercion, field projection or join is inferred. A convenience
  node constructor supports a single table for schema-preserving operators.
- Edges are FORWARD or SHUFFLE. FORWARD requires equal producer/consumer parallelism.
  SHUFFLE specifies all-to-all task connectivity, not record partitioning policy.
- DAGs are nonempty and closed: endpoints exist, IDs/edges are unique, self-edges
  and cycles are rejected. Sources have no input and at least one output; sinks
  have exactly one input and no output; transforms have exactly one input and at
  least one output. Fan-out and multiple independent complete pipelines are valid.
  Multi-input operators are explicitly deferred until an appropriate operator SPI.
- Input collection order does not affect the graph or its topological order. Node
  IDs and successor IDs are traversed in sorted order. The resulting valid order
  is deterministic, not promised to be lexicographically minimal.

## Physical contracts

- One TaskGroup per logical operator; no fusion, scheduling or placement claims.
  Task IDs consist of `(operatorId, subtaskIndex)` with indices 0..parallelism-1.
- Groups follow deterministic logical topological order. A task-group constructor
  validates ownership, count and contiguous index order; task lists are immutable.
- PhysicalPlan retains the immutable logical DAG and exchange descriptors, not a
  quadratic list of network channels. `targetTasks` returns one matching-index
  task for FORWARD or all target tasks for SHUFFLE, and rejects unknown endpoints,
  nonexistent edges and invalid producer indices.
- Planner checks the total task count before allocation. Default budget is 100,000;
  callers may provide a positive explicit budget. Exceeding it fails clearly, not
  via arithmetic overflow or attempted multi-billion-element allocation.
- Graph/plan objects and planner instances are safe for concurrent read/reuse.
  This slice defines no wire protocol or serialized cluster-submission format.

## Out of scope

Operator instantiation, job-config-to-DAG translation, SQL/transforms, task fusion,
record hashing/rebalancing, placement, runtime channels, Master/Worker, transport,
checkpoint coordination, REST and cluster CLI flags. The local baseline remains
unchanged and must continue passing its real subprocess E2E gate.

## Verification

```sh
./mvnw -pl seatunnel-engine -am test
./mvnw -pl seatunnel-cli -am verify
./mvnw -B -ntp verify
```

Tests cover complete chains, branching, disconnected complete pipelines, invalid
shapes, unknown/duplicate endpoints, self-edges/cycles, schema compatibility,
exchange parallelism, deterministic ordering, stable task IDs, bounded allocation,
immutability and concurrent planner reuse. A deep DAG regression guards against
recursive traversal. JDK 17 GitHub Actions runs executable checks because the
sandbox has no JDK or Maven Central access.

## Three-line handoff

Next: Slice 6b adds Master/Worker and task execution using the roadmap-pinned stack.
Bind these planning descriptors to runtime operators; do not add engine types to API.
Wait until this PR is green, reviewed and merged before starting cluster execution.
