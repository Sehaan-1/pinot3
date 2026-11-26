# Slice 6c — Checkpoint decisions, split state and recovery

## Specification and dependency plan

PR title: `[Slice 6c] Replicated checkpoint decisions and explicit recovery`.
Module: `seatunnel-engine`, package `checkpoint`. Dependencies remain engine -> API/common;
no engine imports in API, no connector/core/CLI dependencies.

Adopt existing Hazelcast 5.5.0 IMap atomic entry processors and synchronous backup
for replicated checkpoint records; do not build a storage protocol. Reuse JDK
serialization contracts and immutable collection copies. Reuse JUnit 5.11.0,
AssertJ 3.26.3 and Awaitility 4.2.2 for tests. No new dependency.
Build only the engine-specific checkpoint state machine and split ownership ledger.

## Contracts

- Explicit synchronous orchestration, one caller/coordinator per job, no hidden scheduler.
  A coordinator instance serializes operations. Multiple concurrent masters, split-brain
  fencing, asynchronous barrier alignment and automatic membership-triggered replay are
  NOT supported. Caller must quiesce operators and serialize work with callbacks.
- Named participants return detached Serializable snapshots and prepared commit handles.
  Coordinator collects every participant before publishing a COMMITTING decision through
  an atomic IMap entry processor; publication precedes any external commit. Failed preparation
  aborts all participants best-effort. After publication, failure NEVER aborts: recovery
  retries idempotent commits and completion notifications using stored handles.
- COMMITTING -> COMPLETED compares checkpoint ID/status atomically on the partition
  thread, never arbitrary payload equality or serialization byte order. Checkpoint IDs must be positive and
  strictly increasing per job, including aborted attempts. Local attempted IDs and stored
  decision IDs are checked; after replacement the caller must supply fresh IDs, never
  reuse aborted transaction IDs. Unresolved decisions block new checkpoints. Latest completed
  state restores participants explicitly; participant IDs must match exactly. A failed
  restore is surfaced; caller must discard partially restored operators before retry.
- Participants own transaction semantics, operator confinement, idempotent commit/abort/
  complete and handle cleanup. This is a two-phase decision protocol, not a generic
  exactly-once guarantee for arbitrary sinks. An ambiguous decision-write failure must
  be resolved by reading the store before any abort, and unknown outcomes are surfaced.
- A crash during preparation can leave external orphan transactions: connector/operator
  adapters must support transaction discovery/expiry and explicit cleanup. For an uncertain
  publication, stop submissions, establish the authoritative surviving store outcome and
  recover a present decision; abort prepared resources only after confirmed absence. If
  the cluster outcome is unavailable, operator intervention is required. The uncertain
  coordinator rejects further checkpoints; replace it only after resolving the outcome.
- IMap is replicated in memory, NOT disk-durable storage. Recovery survives coordinator
  replacement while cluster replicas survive, not total-cluster loss. No claim of durable
  API Checkpointed completion: this engine contract is separate and is not automatically
  adapted to connector Checkpointed callbacks. Trusted installed classes only.
- Split ledger validates unique stable IDs, bounds reader IDs, assigns pending work once,
  requeues failed-reader work, snapshots ownership/pending/completed state and restores
  unfinished assignments as pending. Immutable snapshots can be included by participants.
- Explicit delete releases terminal job records; do not delete a job with an unresolved
  decision. Caller is responsible for job lifecycle/retention. No per-checkpoint history;
  latest completed snapshot and at most one pending decision per job are retained.

## Out of scope

Automatic connector deployment, row channels, barrier alignment, distributed operator
mailboxes, automatic task restart, CP fencing, disk persistence, rescaling, periodic
checkpoint scheduling, REST/submission and cluster CLI. Existing local ETL is unchanged.

## Verification

`./mvnw -pl seatunnel-engine -am test` and engine `verify` with real two-member TCP
integration tests: store replication, replacement coordinator, retry after commit failure,
restore and state/handle preservation. Unit tests cover split ownership/requeue/snapshot,
validation and checkpoint failure paths. Also run CLI `verify`, documented local example
and full `./mvnw -B -ntp verify` with check-only formatting/license/coverage gates.

## Three-line handoff

Next: Slice 6d — explicit submission and REST contracts.
Checkpoint decisions survive only while Hazelcast replicas survive; no exactly-once claim.
Start after this slice is green, reviewed and merged.
