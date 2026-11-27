# Programmatic streaming bridge: use and limits

`org.apache.seatunnel.engine.streaming.StreamingRunner` is a synchronous engine entry.
It reuses existing `CheckpointCoordinator` and `CheckpointStore`, not LocalRunner's
bounded-only execution. No new CLI `--mode streaming` or streaming HOCON is accepted.
A trusted engine task may construct connector factories worker-side and call this
entry with `new CheckpointStore(context.cluster())`; don't serialize live readers,
writers, committers, SDK clients or the repository into a task binding. State and
factory classes must be installed on every worker. Source/sink APIs stay engine-free.

## Public entry

`Pipeline<S,R,E,W,T>` takes a nonsecret versioned binding string, configured source and
sink, and reader/enumerator/writer state classes plus committable class. For example,
with the existing adapters on the classpath:

```java
var pipeline = new StreamingRunner.Pipeline<>(
    "fake-console-v1", new FakeSource(3, 10, 1), new ConsoleSink(output),
    FakeSource.ReaderState.class, FakeSource.EnumeratorState.class,
    ConsoleSink.State.class, String.class);
long total = new StreamingRunner().run(checkpointKey, pipeline,
    new CheckpointStore(member), new StreamingRunner.Settings(1000, 1, 1, false));
```

This is illustrative Java within a trusted worker/application, not a CLI config. The
checked-in `StreamingAdapters` test driver runs this path with a serialized TEST store;
`StreamingPipelineIT` uses real engine/Hazelcast (unrun locally). No fake store ships in
production. Fake/Console are engine test dependencies only, not engine production deps.

Settings: checkpoint interval1..3600000ms, idle backoff1..1000ms, maximum emissions per
poll1..10000, `requireCheckpoint` boolean. The three-argument constructor defaults false.
Use false only for initial start or explicitly approved reset. On restart set true to
reject an absent record instead of silently starting fresh after lost state. Keep a
stable unique checkpoint UUID per pipeline/owner; binding must change when relevant
connector configuration, semantics, schema identity or state interpretation changes.
Do not embed passwords in the binding or state. The engine does not compute a config
fingerprint or fence concurrent owners for you.

## Ownership and checkpoint protocol

Single reader0, enumerator and writer0, one caller thread. Synchronous output.write
returns before the reader callback returns; the reader must advance only after output
acceptance. No runtime row queue. Providers must bound their own queues/IO and return
from poll within configured deadlines. Interval checks are between polls; no hard
wall-clock checkpoint deadline or forced SDK cancellation. Interrupt is checked before
poll, per output and after poll; idle sleep is interruptible. Failure/cancellation closes
all created resources and does not force a final success checkpoint. END_OF_INPUT does
perform a final checkpoint with flush(true), then closes. Row count includes prior
checkpointed rows after restore; it is not a unique external-delivery metric.

Checkpoint order:

1. Quiesce between polls; writer.flush(endOfInput), then prepareCommit.
2. Snapshot writer, enumerator and reader, with one binding/schema/row-count/terminal bundle.
3. Existing coordinator publishes COMMITTING under repository's fault model.
4. Idempotent external sink commit; writer → enumerator → reader completion, source last.
5. Mark COMPLETED. No external offset completion before sink commit acknowledgement.

Prepare failure aborts known committables and calls all operator abort notifications,
including partial preparation when no handle returned. Published, conflicting or
uncertain publication is **never** blindly aborted. A commit/notification failure
retains COMMITTING. New run validates binding/schema/types/transactional mode/handles,
retries external commit before new reader/writer creation, restores factory Optional
states, starts operators and repeats completion before polling. During pending recovery
commit may be retried twice; adapters must be idempotent. Failed operator startup keeps
the decision pending. Terminal pending decisions still need live notification; terminal
COMPLETED records return stored count without reopening reader/writer.

Transactional writers must produce independently recoverable handles and never invalidate
published/uncertain prepared handles on close; close discards only unprepared work.
Fresh committers must recover handles without the original writer/SDK client. Abort
callbacks clean partial preparation by checkpoint ID. States and handles must be detached
serializable values, not credential/client references. Nontransactional sinks must return
empty handle lists; output may duplicate after failure. Console is intentionally
nontransactional. JDBC currently rejects checkpoint/restore; passing it here does not
upgrade it, and may fail only at the first checkpoint after some writes.

## Fault model and outstanding work

Hazelcast decisions remain **replicated in memory**, not disk-durable. Backups depend
on available healthy members/configuration; a single member has no spare copy. Whole-
cluster loss, split-brain, concurrent coordinators, automatic fencing/retry/failover and
external orphan reconciliation are unsupported. Operators must maintain one exclusive
live owner/key, fence old attempts before restart and explicitly reconcile uncertain
store/external state. Unknown publication may leave prepared orphan transactions even
if no record is visible. Never blindly clear a decision, reuse transaction IDs or
fall back to earlier state after a commit error.

This slice does not establish generic exactly-once, unbounded JDBC, Kafka/CDC behavior,
parallel/multi-input streaming topology, watermark alignment, transforms/state migration,
remote task submission plumbing or a production disk store. Later connectors need real
service acceptance, recovery, cancellation/backpressure and delivery-specific proofs.
The streaming acceptance matrix remains unchecked, as do previous JDBC/7b gates.

Required commands in an artifact/service-enabled environment:
`./mvnw -pl seatunnel-engine -am verify`, installed CLI regression via
`./mvnw -pl seatunnel-cli -am verify`, and `./mvnw -B -ntp verify`. Those gates, new
Hazelcast IT and JUnit are unrun locally. No Actions or external spend is granted by
these instructions. See [slice specification/results](SLICE-7E0.md).
