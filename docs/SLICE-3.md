# Slice 3 — Engine-independent API

## Specification and dependency plan

PR title: `[Slice 3] Engine-independent seatunnel-api contracts`.

The `seatunnel-api` module owns `org.apache.seatunnel.api` packages `table`,
`configuration`, `source`, `transform`, `sink`, and `state`. Its only production
module dependency is `org.apache.seatunnel:seatunnel-common:3.0.0-SNAPSHOT`.
No config, engine, discovery, connector, Spark, or Flink imports are permitted.

Build vs adopt: the SPI and its value objects are project-specific contracts, so
implement them with JDK 17 types. Do not invent parsers, schedulers, serialization
protocols or execution machinery. Existing Jackson/HOCON facades remain in the
foundation layer. There are no new runtime libraries. Tests adopt the root-pinned
`org.junit.jupiter:junit-jupiter:5.11.0` and
`org.assertj:assertj-core:3.26.3`.

## Contracts

- Rows have positional fields, defensive array copying and deep value equality.
  Field values themselves are shallow-copied and remain caller-owned; wire formats
  are deliberately deferred. Serializable row fields are required for Java transport.
- Catalog tables have immutable, ordered columns, unique nonblank names, logical
  scalar types and nullability. Decimal precision and richer types are future API
  extensions, not silently approximated here.
- Typed options support required keys or non-null defaults. Configuration parsing
  and coercion are not part of the API. Option maps are copied, values shallowly.
- Sources create readers and enumerators with optional restored snapshots. Stable
  split IDs, boundedness, assignment and end-of-input signaling are explicit.
- Reader polling returns more-available, nothing-available or terminal end-of-input;
  it must not block indefinitely. Scheduling/backoff belongs to the future runtime.
- Transforms can produce zero, one or multiple rows and expose their output schema.
- Writers flush, snapshot and prepare detached checkpoint commit handles. Optional
  committers commit/abort idempotently. The SPI does not itself promise exactly-once
  delivery; that requires connector support and the later checkpoint coordinator.
- Snapshots and split payloads are serializable connector-owned values. Snapshot
  implementations must detach mutable state before returning it.
- Runtime lifecycle calls on each operator instance are serialized. Factories are
  configured before use; no implicit cross-thread safety is promised. The runtime
  owns closing created resources even on failure, and must propagate exceptions.

## Scope boundary

No fake production operators, runnable pipeline, plugin discovery, format codecs,
engine scheduling, checkpoint coordinator or connector implementation. Those remain
in their roadmap slices. Test-local implementations exercise contracts only.

## Verification

```sh
./mvnw -pl seatunnel-api -am test
./mvnw -B -ntp verify
```

JUnit tests cover value semantics, serialization, defensive copies, invalid schema
and options, and the no-engine-import boundary. CI runs JDK 17, formatting, strict
license checks and JaCoCo reporting. Local Maven is unavailable in the sandbox.

## Three-line handoff

Next: Slice 4 adds plugin discovery and formats against these contracts.
Adopt ServiceLoader and the roadmap-pinned format libraries; keep engine imports out.
Do not start Slice 4 until this PR is reviewed, green and merged.
