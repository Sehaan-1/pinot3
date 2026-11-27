# Stage 7 completion program

## This planning PR

Title: `Lock full Stage 7 completion scope and delivery gates`.
Files: documentation only (this plan, roadmap, README, CI ledger). No production
modules/dependencies change, no new cycles, SDK adoption or service resources. Contract:
replace stale estimated PR identities with actual progress and an explicit outstanding
completion matrix. Out of scope for this PR: implementing or claiming any unchecked
connector/runtime/advanced-transform row. Local verification: Markdown file-link
existence and git diff --check; no Maven/runtime verification needed for docs-only diff.

## Authorization and current status

The user selected **all remaining Stage 7** after PR21 was merged, not another isolated
SQL operator. Scope includes advanced transforms and JDBC, Kafka, files, Redis, MongoDB,
HTTP, CDC, S3, GCS and Azure connectors. Optional Spark/Flink translation stays deferred.
This document is the implementation/dependency/acceptance plan, **not implementation**
of those remaining features. All unchecked items below remain outstanding.

Keep one cohesive deliverable per PR; do not combine unrelated SDKs or generate empty
connector modules/providers to simulate progress. No automatic merging or new Actions.
The previous CI grants were one-time: reserved25m / observed14m45s, fully allocated.
PR16's reactor verification at032f66f covers through7a6; subsequent7a7–7a11 have local
supplemental evidence and explicit unverified merges only. A full verification gap remains.

## Scope lock

- Existing7a transform/filter/expression surface is feature-frozen for this program.
  Fix regressions, but do not keep adding SQL operators as extra completion slices.
- Complete7b advanced transforms, runtime prerequisites and7d–7k connectors below.
-7c translation remains explicitly optional/deferred; not a hidden completion criterion.
- Stage8 shared E2E harness and Stage9 distribution are separate. Each connector still
  needs real service E2E now; do not defer its correctness to a future harness project.
- A bounded demonstration is not a completed unbounded/transactional connector.

## Dependency facts checked in this checkout

1. `LocalRunner.execute` accepts BOUNDED sources only and rejects a nonempty sink
   committer. It is not an unbounded streaming runtime or checkpoint/transaction driver.
2. `SeaTunnelTransform` remains unary. Slice7b3 adds bounded multi-input UNION ALL
   in core LocalRunner, not in a unary plugin. No watermarks, joins, streaming merge
   coordination or distributed DAG integration is claimed.
3. At the planning checkpoint `SeaTunnelRow` lacked native row-kind metadata. Slice7b1
   adds that engine-independent carrier and a real metadata extractor with preservation
   through existing transforms. Local checks only; Maven gates remain pending. CDC
   envelope translation and row-kind-aware external sink semantics are still absent.
4. Engine checkpoint decision/restore primitives exist, but that is not an integrated
   arbitrary-connector recovery/exactly-once pipeline. Prove the integration before
   claiming those delivery guarantees.
5. No Docker executable is present in this sandbox. Maven Central is outside outbound
   allowlist. External-service/container tests cannot currently be executed locally;
   source-built HSQLDB smoke checks are not PostgreSQL/MySQL/Kafka/CDC E2E evidence.

These prerequisites belong in core/engine/API layers first when needed. The API must
retain zero engine/connector imports. Do not hide scheduling/protocol implementations
inside connector modules to bypass the layering order.

## Ordered delivery and acceptance matrix

No future PR numbers are preassigned. A checkbox requires implementation **and** its
verification evidence, not merely an opened PR or a library dependency in a POM.

| Done | Deliverable | Adopt / adapt (pins at implementation entry) | Public contract / required real E2E |
|---|---|---|---|
| [ ] |7b advanced unary transforms | Existing Guava/Commons/Jackson/format adapters; no bespoke token/JSON parser | 7b1 native row-kind/extractor and7b2 exact-token filter are implemented with local supplemental checks. Maven/JUnit/packaged CLI gates remain unrun; do not check off this bundle until verified. All named unary 7b implementations are present; verification remains incomplete. |
| [ ] | Multi-input prerequisite + TableMerge | Existing DAG/task/transport primitives; library serialization; no new scheduler in transforms | Slice7b3 implements bounded schema-aligned UNION ALL in core, serial inputs/shared chain and writer, preflight validation, backpressure/end-of-input/failure cleanup. 207 local contracts + real multi-source E2E passed; Maven/JUnit/installed CLI/full reactor unrun. No streaming or distributed merge claim. |
| [ ] |7d JDBC PostgreSQL source + sink | HikariCP + pgjdbc (Apache-2.0 / BSD-style); JDBC prepared statements | Slice7d1 implements bounded range partitions, explicit schema/typed bindings, Hikari ownership, batch commit/rollback and explicit unsupported restore. Spec and gated real PostgreSQL/CLI tests added.195 proxy checks + HSQLDB exercise passed; actual PostgreSQL/Hikari/pgjdbc/Maven/CLI gates UNRUN, so not complete. |
| [ ] |7d MySQL adapter and driver decision | JDBC/HikariCP reuse; driver license/distribution review first | Slice7d2 implements jdbc-mysql by adapting existing JDBC lifecycle/dialect, with explicit external official Connector/J26.10.0 GPLv2+exception policy and test-only opt-in profile.102 local dialect/proxy checks passed; actual MySQL/driver/Hikari/CLI/Maven gates UNRUN. Gated InnoDB roundtrip/metadata/quoting/NULL/rollback tests and external-classpath CLI test added; completion still unchecked. |
| [ ] | Streaming/checkpoint runtime prerequisite | Existing engine/transport/state primitives, no connector-owned coordinator | Unbounded poll/cancellation/backpressure, checkpoint->source offset/sink flush ordering, close/restart/restore tests with real adapters. No generic exactly-once promise without corresponding external protocol. |
| [ ] |7e Kafka source + sink | Apache Kafka clients (Apache-2.0); Testcontainers Kafka | Partition assignment, offset snapshots/restore, key/value/schema format, bounded offset mode and live mode separately, delivery/retry/transaction claims explicit. Real broker E2E, restart/offset recovery and failure cleanup. |
| [ ] |7f local file connector | JDK NIO for filesystem, Commons CSV/Jackson/Apache Parquet/Apache ORC libraries for formats | CSV/JSON/Parquet/ORC/Text supported and tested individually, path safety, bounded split ownership, charset/framing, atomic publication/flush/cleanup. Real temporary-directory round trips for every format. Existing parquet metadata structures are not a Parquet reader/writer. |
| [ ] |7g Redis source + sink | Jedis (permissive license checked at pin); Testcontainers Redis | SCAN consistency/duplicates/cursor semantics explicit, bounded reader and typed key/value mapping, write/TTL/batch/retry semantics. Real Redis E2E, disconnect and restore tests; no snapshot-isolation fiction. |
| [ ] |7h MongoDB source + sink | Official MongoDB sync SDK (Apache-2.0); Testcontainers MongoDB | BSON/schema mapping, bounded query split/resume contract, batched writes/idempotency, cursor/client cleanup. Real MongoDB E2E and partial-batch failure tests; CDC belongs in7j. |
| [ ] |7i HTTP source + sink | OkHttp (Apache-2.0), Jackson, WireMock/MockWebServer | Pagination, timeout/TLS/auth/redaction/body limits, status/retry/idempotency contract, cancellation and connection/body cleanup. Real HTTP server E2E (not a mocked transport); no custom HTTP/retry protocol. |
| [ ] |7j CDC | Debezium embedded/engine + database connector SDKs (Apache-2.0) | Change envelope/row kind, durable offsets/schema history, snapshot+stream transitions, backpressure and engine checkpoint alignment. Real PostgreSQL logical replication and MySQL binlog tests, insert/update/delete/restart. No hand-written log decoder or exactly-once claim by default. |
| [ ] |7k S3 | AWS SDKv2 + existing file/format layer; permissive licenses | Object listing/splits/pagination, credentials/endpoint configuration, multipart/abort/publication semantics. Real S3-compatible LocalStack E2E and failure cleanup; remote-listing consistency explicit. |
| [ ] |7k GCS | Official Google Cloud Storage SDK + shared format layer | Listing/resume/generation conditions, upload/abort/retry and auth redaction. Supported emulator E2E plus a separately gated real GCS acceptance check; an emulator is not cloud equivalence. |
| [ ] |7k Azure Blob | Official Azure Storage Blob SDK + shared format layer | Listing/continuation, ETag/lease/block publication/auth/cleanup. Azurite E2E plus separately gated real Azure acceptance check; no live cloud credentials committed. |

The order is dependency-driven, not a promise to implement streaming before its
runtime exists. JDBC is the first connector implementation; file/format contracts
must exist before object-storage adapters. CDC waits for streaming/checkpoint and
change-metadata contracts. Independent connectors still remain separate PRs.

## Dependency and license policy

- Pin exact Maven coordinates centrally at each module's implementation entry, after
  checking JDK17 compatibility, transitive convergence, license and known security
  advisories. Do not freeze every future SDK version months before adoption.
- Prefer permissive official SDKs. MySQL Connector/J GPL+exception and MariaDB LGPL
  are not silently classified as permissive; decide driver packaging/distribution
  explicitly before adding either dependency or claiming MySQL complete.
- Testcontainers/JUnit/AssertJ remain test-only. Docker/container images must be pinned
  and failure diagnostics collected. No tests auto-start Actions or download secrets.
- Slice7d1 adopts HikariCP7.1.0 and pgjdbc42.7.15 after tagged source/license/API
  review. Hikari targets Java11; root SLF4J aligned to2.0.17. pgjdbc checker-qual
  aligned with Guava at3.55.1. Actual Maven convergence remains unrun.
- Slice7d2 records official Connector/J26.10.0 GPLv2+exception as an operator-owned
  external driver, not default runtime/shaded content. An explicit test-only Maven
  profile opts into that dependency. No permissive classification or legal exemption
  is inferred from classpath separation; full policy in [MYSQL-DRIVER.md](MYSQL-DRIVER.md).
- Never implement a wire protocol, HTTP retry engine, CSV/Parquet/ORC codec, binlog
  reader, cloud signer or database pool where the adopted library already owns it.

## Common contract gate for every connector PR

1. Written spec/adoption plan first: module, exact pinned coordinates/licenses, options,
   supported schema types, row kinds, validation/redaction and precise scope/out-of-scope.
   Append-only external sinks must reject unsupported change kinds, not silently turn
   DELETE/UPDATE_BEFORE into inserts.
2. No resources opened in discovery/config/schema factories. Runtime owner creates and
   closes client/pool/reader/writer resources, including partial-start and error paths.
3. Bounded blocking/polling/timeouts/cancellation specified; no unbounded hidden buffers.
4. Explicit split/state validation, emission/offset advance ordering, flush/prepareCommit/
   external commit ownership. Unsupported restore/transactions reject rather than no-op.
5. State snapshots detached and serializable; credentials/SDK clients/connections absent.
   API engine-independent. Thread-confinement or synchronization contract documented.
6. SDK/driver does IO/protocols/retries. The adapter handles SPI/schema/ownership only.
7. Installed ServiceLoader providers, CLI dependency/packaging and runnable configuration
   examples. External services/credentials supplied explicitly, no misleading demo URL.
8. Tests for types/NULL/arity/schema/options/lifecycle/cancellation/partial failures,
   public contracts, checkpoint/restart where supported, and real service round trip.
9. Affected-module Maven test, packaged CLI E2E, full reactor verify and dependency/
   license/format checks. Name unrun gates; mocked/source-built substitutes are not passes.
10. PR summary and three-line handoff; separate review/merge authorization. A future
    "next" continues implementation, not automatic merge or new CI budget.

## Verification policy with no remaining Actions allocation

Implementation may proceed locally, and PRs can collect tests/documented manual commands.
A connector is not complete just because tests were written. If Docker/Maven/service
access is unavailable, mark the relevant gate **BLOCKED/UNRUN**, not green. Further
Actions or real-cloud spend requires explicit user authorization; do not silently
reuse the previous one-time exceptions or infer a fresh allowance from this program.
Do not progress into dependent implementations before required upstream contracts
are merged/verified, except a new explicit scoped waiver from the user.

## Completion definition / honest handoff

Stage7 is complete only when every mandatory row above is implemented, reviewed,
merged and has its required verification evidence, with all delivery guarantees
accurate and all known blockers resolved. Optional7c is listed as deferred, not done.
There is no Stage7 completion claim in this planning PR. Current production progress:7b1 native row-kind carrier/extractor/preservation and
7b2 literal-token filtering and7b3 bounded multi-input TableMerge, with supplemental
local checks only. All named7b implementations are present; gates remain unrun.
Slice7d1 PostgreSQL source/sink is implemented but actual PostgreSQL/Hikari/pgjdbc and
Maven/installed CLI gates remain unrun. JDBC verification gaps remain.
Slice7d2 MySQL dialect/driver policy is implemented but service/driver/Hikari/packaging/
Maven gates remain UNRUN. Next: streaming/checkpoint runtime prerequisites, with all
JDBC verification gaps carried forward. No mandatory row above is fully complete/verified yet.
