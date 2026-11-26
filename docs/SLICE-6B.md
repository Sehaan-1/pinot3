# Slice 6b — Embedded Master/Worker and task execution

## Specification and dependency plan

PR title: `[Slice 6b] Hazelcast Master/Worker and bounded remote task execution`.

Extend `seatunnel-engine` with `cluster` and `task` packages. Keep engine -> API /
common only; no engine imports in API and no core/CLI/connector dependency.

Build/adopt decisions:
- Adopt `com.hazelcast:hazelcast:5.5.0` for membership, oldest-member coordination,
  member selection, remote callable serialization/dispatch and transport. Use its
  bounded named executor rather than inventing an RPC protocol or scheduler.
- Adopt `io.micrometer:micrometer-core:1.13.4` via the existing BOM for worker counters,
  timer and pool gauges; reuse SLF4J 2.0.16. Registries supplied by callers remain
  caller-owned; default registries are node-owned.
- Use JDK ThreadPoolExecutor + ArrayBlockingQueue + FutureTask for bounded local
  execution, interruption and cleanup, not a custom thread pool.
- Adopt test-only `org.awaitility:awaitility:4.2.2` (Apache-2.0) for bounded
  asynchronous assertions instead of homemade polling utilities. Reuse JUnit 5.11.0,
  AssertJ 3.26.3 and Failsafe 3.2.5. Embedded integration tests
  exercise real Hazelcast TCP members, not mocks or external infrastructure.
- Netty remains root-pinned but is not an unused dependency. Hazelcast handles this
  slice's network transport; a separate data-plane transport must be justified in
  a future slice rather than maintaining two control-plane protocols.

## Contracts

- EngineNode owns a Hazelcast member and TaskExecutionService. Settings explicitly
  select cluster name, bind interface, port and TCP seed addresses; multicast,
  auto-detection, Jet, phone-home and Hazelcast shutdown hooks are disabled. Closing
  one node never calls global shutdownAll or closes another node's registry.
- The oldest visible member may submit through MasterService. This is a single
  healthy-cluster coordinator, NOT CP-fenced leadership or split-brain protection.
  There is no durable master failover or automatic task replay in this slice.
- Every physical TaskId must have exactly one Serializable EngineTask binding.
  All bindings are validated before dispatch. Tasks execute concurrently, as required
  for future pipelined operators; DAG edges do not impose completion barriers.
- Default placement delegates to Hazelcast among engine members. An explicit target
  UUID supports pinned placement and deterministic testing. Non-leaders, unknown
  workers, incomplete bindings and closed services reject submission clearly.
- RemoteTask dispatches into the receiving member's bounded TaskExecutionService.
  Duplicate active TaskKeys, saturated queues and post-close submission are rejected.
  TaskContext provides the task key, worker UUID and the local Hazelcast member for
  trusted engine task code. TaskOutcome records the actual executing member.
- TaskId/TaskKey and outcome envelopes use standard Serializable contracts through
  Hazelcast; task classes must exist on every member. This is not a portable job
  submission format, arbitrary-code upload API or security sandbox.
- JobExecution is an owned Future-like handle: await/status observe completions,
  propagate remote causes and cancel siblings on observed failure. There is no
  hidden per-job polling thread. Callers must await/poll or close handles; automatic
  background failure supervision is not claimed. MasterService retains a bounded
  number of handles, reaps terminal ones on submission, and cancels all on close.
- await has one overall timeout and preserves interruption. Timeout, interruption,
  failure and explicit close trigger best-effort remote Future.cancel(true), which
  interrupts local worker work through the remote wait. Tasks must cooperate with
  interruption; Java cannot forcibly stop arbitrary user code. Cancellation is not
  rollback, exactly-once execution or proof that remote side effects have ceased.
- Worker shutdown cancels queued/running work, shuts down its pool and waits a
  bounded duration. Non-cooperative tasks cause an explicit shutdown error rather
  than a false clean-stop claim. Metrics are worker-tagged, never job/task-tagged,
  and owned meters are removed during shutdown.

## Security and scope boundary

Only deploy on a trusted private network with trusted task classes on every member.
Cluster names are NOT authentication. This baseline does not enable TLS, untrusted
multi-tenancy, client access controls or sandboxing; public-internet deployment is
unsupported. Production network hardening requires a separately reviewed scope.

No automatically wired distributed ETL pipeline, row routing, connector loading on
workers, data-plane channels, checkpoint coordinator, recovery, REST or cluster CLI.
The existing local FakeSource -> ConsoleSink pipeline stays unchanged and verified.

## Verification

```sh
./mvnw -pl seatunnel-engine -am test
./mvnw -pl seatunnel-engine -am verify
./mvnw -pl seatunnel-cli -am verify
./mvnw -B -ntp verify
```

Unit tests cover bounds, duplicate keys, metrics, errors, cancellation and shutdown.
Failsafe integration tests start two real TCP members, pin execution to the other
member, execute bound physical tasks, propagate failures, cancel blocking work,
observe worker metrics, enforce leader/binding validation and verify node ownership.
All tests shut down owned members and executors in finally/try-with-resources.
JDK 17 GitHub Actions is the executable gate; this sandbox lacks JDK/Maven Central.

## Three-line handoff

Next: Slice 6c introduces checkpoint/state coordination and recovery as its own PR.
Do not infer durable execution or exactly-once behavior from a completed remote task.
Start only after this PR is green, reviewed and merged.
