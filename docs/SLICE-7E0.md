# Slice 7e0 — single-reader streaming/checkpoint runtime bridge

## Specification and reuse decision (before implementation)

Engine prerequisite, not Kafka/CDC or a new connector. Reuse existing API reader/
enumerator/writer/committer, CheckpointCoordinator/PreparedState/CheckpointRecord and
Hazelcast CheckpointStore. Extract engine-only CheckpointRepository contract for the
existing store and deterministic failure tests; do not create a second coordinator,
scheduler, serializer or production in-memory/file store. No new production dependencies
or engine imports into API. Coordinator constructor widens to repository interface:
existing CheckpointStore source callers remain valid; compiled clients must rebuild.

StreamingRunner is a programmatic engine entry, single source/reader0/sink writer0,
caller-confined cooperative poll loop, synchronous write backpressure, no row queue.
Both bounded and unbounded adapters accepted; unbounded may signal terminal input.
Periodic checkpoint checks use System.nanoTime between polls; idle Thread.sleep is
JDK backoff, not a new scheduler. Enforce interruption/row arity/maximum emissions per
poll. Connector poll/IO must return within its own deadlines; runtime cannot forcibly
interrupt arbitrary blocking SDKs. No parallel DAG/union/watermarks/transforms or CLI
streaming flag in this slice. LocalRunner/CLI still bounded. Stream-capable adapters
must implement real detached state/restore; JDBC deliberately rejects this lifecycle.

One engine bundle participant atomically snapshots source reader+enumerator+sink writer,
row count, schema, explicit versioned binding identity, terminal flag and committer
presence. Typed state/handle class tokens validate restore before reader/writer factories open.
Checkpoint: quiesce between polls -> writer.flush(final-input flag) -> prepareCommit
-> writer snapshot -> enumerator snapshot -> reader snapshot -> existing replicated
COMMITTING decision -> idempotent external commit -> writer/enumerator/source completion
notifications (source last) -> COMPLETED. No source completion before sink commit.
Pre-publication prepare failure aborts known handles and notifies all operators; after
published/uncertain decision NEVER abort, retain decision for operator-led recovery.
Source emits/offset advancement only after synchronous downstream accepts per SPI.

Restart: validate stored binding/schema/state/transactional mode/handles, create
committer, recover pending external commit BEFORE creating reader/writer/enumerator,
restore bundle into their factory Optional states, start reader/enumerator, re-notify
last completion idempotently, then poll. Terminal completed bundle returns stored count
without reopening operators (committer may open for recovery); terminal pending decisions
still restore live operators for completion acknowledgement. Restart contract requires
fresh configured factories; no automatic retry or rewind inside a live failed instance.
No-store case starts fresh. Snapshot payloads/committables must be detached/serializable;
repo/classes trusted, not an untrusted deserialization entry point.

Transactional adapter contract: prepareCommit must return independently recoverable,
idempotently committable handles; close cannot invalidate published/uncertain prepared
handles. Abort notifications clean partial prepare even when no handle was returned.
Nontransactional sinks return no handles; output can duplicate after failures. No generic
exactly-once promise. Hazelcast decisions remain replicated IN MEMORY, not disk-durable;
whole-cluster loss and concurrent/split-brain coordinators are not solved. One fenced-by-
operator/exclusive live owner per checkpoint key is a deployment prerequisite, not a
runtime fencing implementation. Explicit store-loss decision required, never promise HA.

Tests: executable deterministic repository with Java serialization (test-only) for poll/
flush/snapshot/commit/notify ordering, restore, failures, unknown publication, cancel/
idle/backpressure/arity/emission cap, terminal restart and partial-start cleanup. Real
FakeSource -> ConsoleSink through this runner, plus real Hazelcast store integration test.
Existing11 LocalRunner examples regress, not packaged CLI evidence. New Maven/JUnit/Hazelcast/full reactor gates
unrun unless actually executed. No Actions authorized. User next-then-merge applies this
single prerequisite PR with disclosed gaps; no Kafka/CDC completion or new CI permission.


## Implementation refinements and results

Pending recovery deliberately pauses coordinator completion with an internal marker
after external commit acknowledgement. It then creates/starts restored live operators
and retries idempotent recovery+notifications; only then can COMPLETED be published.
This includes terminal pending decisions: skipping reader notification would lose source
completion, so those restore operators but never poll new rows. Fully completed terminal
snapshots return without reopening reader/writer. Failed startup during recovery retains
the pending decision. requireCheckpoint=true refuses missing restart state; false is
initial/explicit reset only. It cannot distinguish store loss on its own. No false
persistent durability: configured Hazelcast backups are in memory, one member has no
spare copy, and split-brain/concurrent-owner/whole-cluster-loss recovery is unsupported.
Unknown publication may orphan prepared external handles: never auto-abort them; fence
owners and reconcile the external system/store manually.

310 checked-in executable assertions passed under JDK17: emission->synchronous write->
advance, flush/prepare/sink+enumerator+source snapshot/publication/commit/notification
ordering, serialized state restore, pending commit and lost-ack idempotency, partial
recovery startup, terminal pending notification, completed terminal restart, prepare/
snapshot/commit/notification/publish-before/publish-after/conflict/store-complete failures,
predecision abort vs published/uncertain no-abort, per-resource startup/close failures,
nontransactional duplicate semantics, idle checkpoint/cancellation/interruption flag,
arity/emission caps, missing checkpoint refusal and binding/transactional mismatch.

Real existing FakeSource -> new bridge -> ConsoleSink terminal and mid-input restore passed
exact rows.927 earlier assertions and11 prior LocalRunner pipelines passed. The test
repository serializes using JDK Object streams for detached roundtrips, not an invented
production serializer/store. Test committer models idempotency but proves no actual
external transaction semantics. New runner/repository/coordinator/records compile from
unchanged production files (no import relinks/stubs) on JDK17. Fake/Console execution
retains prior temporary shaded Jackson format adapter; existing regression libraries
remain source-built/shaded, not pinned Maven proof. Fake/Console dependencies TEST only.

UNRUN: added JUnit, real Hazelcast store/engine IT and previous engine recovery IT
against widened constructor, affected API/engine Maven, installed CLI, full reactor/
convergence. Actual Hazelcast-dependent store compilation is unrun; only interface and
coordinator production subset compiled locally. No Actions. Formatter1.19.1/parsing,
POM XML/test-scope/Java headers/local Markdown links/diff checked. Constructor widening
is source-compatible for old store callers, not a binary compatibility promise.

Implemented: single-reader checkpointed programmatic engine pump and operator bridge.
Pending: actual Hazelcast/Maven/JUnit/full reactor and real unbounded connector recovery.
Next: Kafka integration; no completed streaming/JDBC/Stage7 or blanket merge/CI claim.
