# Slice 7e1 — complete Kafka adapter implementation scope

## Specification / dependency plan before implementation

One cohesive connector-kafka PR: installed source and sink, bounded and live reads,
checkpoint/restore, acknowledgement-bounded sink, formats/security/options, CLI/examples,
runtime/broker/packaging tests and documentation. Reuse official
org.apache.kafka:kafka-clients:4.4.0 (Apache-2.0, tagged2026-10-06) for protocol,
assignment, offsets, serialization, producer buffering/retries/TLS/auth. Existing binary
row schema/JDK maps and StreamingRunner checkpoint bridge, no custom broker protocol,
retry engine, scheduler, coordinator, codec or engine-specific API imports.
Tagged LICENSE/build/client signatures reviewed; GitHub exact-version advisory query
returned no entries (not a full security audit). Root SLF4J2.0.17 manages Kafka's1.7.36
request. Preserve Kafka client codec dependencies (zstd1.5.7-17, lz4-java1.11.2,
snappy1.1.10.7 from tagged build); don't replace decompression or blindly drop natives.
Actual artifacts/transitive convergence/packaging/service verification remain gates.

## Source contract

Identifier kafka. Strict common options bootstrap-servers (1..16 strings), topic,
security-protocol (SSL default, explicit PLAINTEXT or SASL_SSL), optional JKS truststore
location/password pair and keystore location/password/key-password triple, and SASL
mechanism PLAIN/SCRAM-SHA-256/SCRAM-SHA-512 + opaque SDK-owned JAAS config together
only for SASL_SSL. No arbitrary client properties, Java login parser or secret-bearing
toString. Adapter database/client failures sanitized without driver causes; CLI disables
raw Kafka logging, embedding owns logging policy. Secrets resolved externally.

Source adds group-id, mode exactly bounded/live, partitions (1..128 distinct entries:
partition >=0, start-offset >=0; bounded requires end-offset >=start, live forbids end),
poll-ms1..1000 default100, max-poll-records1..1000 default100, timeout-ms1000..60000
default10000, max-record-bytes1..16777216 default1048576. Source schema exactly topic
STRING, partition INT, offset LONG (nonnullable), key/value nullable BYTES. Raw bytes
only; tombstone value null retained, no text/JSON/Avro conversion or changelog inference.

Manual assign of configured partitions to reader0, no subscribe/rebalance/auto-discovery;
new partitions ignored until explicit versioned job change. Source boundedness differs
by mode; LocalRunner rejects live before runtime IO. Bounded ranges are half-open and
must already be readable; validate beginning/end (read_committed LSO) to reject expired/
out-of-range offsets. No silent offset reset, no restore from broker group fallback.
SDK read_committed with auto.commit=false; explicit seek. One bounded fetched batch,
one emitted row per poll. Track accepted next offsets, NOT prefetched consumer positions
while buffered records remain. Only after whole batch is accepted/skipped reconcile
positions for compacted/control/aborted gaps; clamp/pause finished bounded ranges.
No global cross-partition ordering/snapshot; at most configured per-partition bounds.

Reader state: detached configured splits/group/accepted offsets/assignment/checkpoint id;
enumerator state: detached splits + assignment. Validate binding/bounds/exact partition
sets before IO; restore assigned partitions/seek from reader state without reassigning
from restored enumerator. Snapshot mid-batch replays only not-yet-accepted records.
Checkpoint completion commits the EXACT saved offsets via commitSync after sink ack
(StreamingRunner source-last ordering), including restored/duplicate notifications.
Aborted checkpoints don't commit group offsets. Engine state authoritative; group
commits advisory, exclusive group/partition/checkpoint owner required. No fencing.

## Sink contract

Identifier kafka. Common/security/timeout/max-record options plus key-field/value-field
(default key/value) selecting existing nullable BYTES columns. Other columns opaque,
exact arity, INSERT only, defensive byte copies; tombstones allowed, no row-kind->
tombstone conversion. max-in-flight-records1..1000 default100. Kafka SDK owns idempotent
acks=all retries within delivery deadline; no transactional.id, no external committer,
no cross-restart exactly-once claim. Bounded future queue; when full await an ack before
accepting more. Flush waits every future with finite deadline; snapshot requires all
acks. WriterState acknowledged count, strict restore, prepareCommit returns no handles.
Failure terminal, close(Duration.ZERO) best effort cancels unacked sends (may already
be delivered). Restoring source replays since last checkpoint: duplicates possible.
Kafka producer session idempotence isn't transactional source-to-sink exactly-once.

## Lifecycle / integration / gates

Factories no IO. Runtime opens/owns SDK clients; single-thread confinement, start once,
idempotent close including partial start/failure, thread interruption checks; no hard
wall-clock/DNS cancellation promise. Small package-private client-operation port solely
adapts official SDK calls for deterministic tests; no broker/pool/protocol replacement.
Limits don't sandbox hostile SDK batches (broker can return an oversized first batch).

Install ServiceLoader/CLI dependency, bounded copy and Kafka->Console examples; live
programmatic StreamingRunner recipe. Tests include config/schema/NULL/kinds/ownership/
limits/lifecycle/client failures, SDK MockConsumer/MockProducer delegation, bounded
range/gap/mid-batch restore/exact completion offsets, sink ack failures/backpressure,
real broker bounded/live/restart/group commits/failure cleanup, streaming bridge and
packaged CLI roundtrip. Broker gates explicit environment, never auto-start Actions.
If artifacts/broker/Docker unavailable, report UNRUN, not green; client-port/proxy/source
checks aren't real Kafka acceptance. All implementation scope in this PR, but Kafka/
streaming/Stage7 remain unchecked until actual service/build gates pass. User request
next-then-merge covers this one entire Kafka implementation PR, no new CI/future merge
permission. Previous JDBC/7b/runtime acceptance gaps remain carried forward.

## Implementation result / evidence

Implemented connector-kafka module, official SDK operation adapters, strict resource-free
providers, source/enumerator and sink state, CLI service discovery/dependency/log suppression,
examples and [operator guide](KAFKA.md). PASS108 port assertions and real StreamingRunner
live cancellation/serialized restore using test-only ports/repository. Supplemental JDK17
compile excludes production KafkaSdk and substitutes only an uncommitted factory boundary.
SDK mock, broker/Testcontainers, actual Hazelcast runtime broker restart, packaged CLI and
logging tests are added but UNCOMPILED/UNRUN; actual pinned SDK/client dependencies, Maven,
full reactor, formatter1.19.2 and service gates remain UNRUN. Local formatter1.19.1 and XML/
whitespace checks only. No Actions used. Physical broker outage/restart/TLS/SASL matrix
not claimed. Full implementation scope shipped together, acceptance deliberately unchecked.
