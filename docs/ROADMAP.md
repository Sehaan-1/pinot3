# Slice Roadmap — SeaTunnel Clone

> **How to read this doc.** Each row is exactly one PR. A PR merges only when
> its verification command is green in CI. Downstream PRs never start before
> their upstream contracts are merged. Versions below are pinned in the root
> POM's `dependencyManagement` at the slice that first adopts them; connector
> SDK versions are pinned in their own PR (not upfront) to avoid stale pins.

## Current Stage 7 program

The user authorized **all remaining Stage7** after PR21 merged. The authoritative
remaining scope/dependency/acceptance checklist is
[Stage7 completion program](STAGE-7-COMPLETION.md). No more isolated SQL operator
expansions are planned as a substitute for advanced transforms/connectors. Translation
remains optional/deferred. This is a plan, not a claim those connectors are implemented.

PR numbers in the original roadmap were estimates, not reserved identities. The
index below distinguishes actual merged PRs from planned future deliverables.
PR16 verified through7a6; later7a7–7a11 have supplemental checks/unverified merges.
No new Actions allocation or automatic merge permission accompanies this program.

## Ground rules (apply to every PR)

- **One slice = one PR.** Single responsibility, bounded diff, one verification command.
- **Reuse first.** Adopt battle-tested libraries; write only thin adapters.
  Custom code is justified only where no proven library fits SeaTunnel's SPI.
- **Layering.** `seatunnel-api` → zero engine/connector imports.
  `seatunnel-engine` depends on `seatunnel-api`, never the reverse.
- **Green main.** `./mvnw -B -ntp verify` (Spotless + license + tests + JaCoCo)
  must pass before merge. No silent stubs or dummy `TODO`s.

## Dependency order

```
PR #1  Scaffold (root POM, wrapper, CI)
  └─ PR #2  Common + Config
       └─ PR #3  API (contracts)
            ├─ PR #4  Plugin discovery + Formats
            │    └─ PR #5  Core + CLI + Fake + Console  (first runnable pipeline)
            │         └─ PR #6–#9  Engine / Zeta (DAG → cluster → checkpoint → submit/REST)
            │              └─ Stage7a/7b  Transforms v2
            │                   └─ Stage7d–7k  Connectors (one per PR, real service E2E)
            │                        └─ PR #21  E2E harness   └─ PR #22  Distribution
            └─ Stage7c  Translation (Spark/Flink) — OPTIONAL, deferrable
```

## PR index

| PR | Slice | Modules / packages | Libraries adopted (Maven coordinates) | Contracts locked | Verification |
|----|-------|--------------------|----------------------------------------|------------------|--------------|
| #1 | 1 Scaffold | root only (no modules) | Maven 3.9.9 (wrapper), JDK 17, `com.diffplug.spotless:spotless-maven-plugin:2.43.0`, `com.mycila:license-maven-plugin:4.5`, `maven-enforcer-plugin:3.5.0`, `jacoco:0.8.12`, Surefire/Failsafe `3.2.5` | Coordinates `org.apache.seatunnel:seatunnel:3.0.0-SNAPSHOT`; Java 17; BOM layout; CI gate | `./mvnw -B -ntp verify` |
| #2 | 2 Common + Config | `seatunnel-common`, `seatunnel-config` | `com.google.guava:guava:33.2.1-jre`, `commons-lang3:3.17.0`, `commons-collections4:4.4`, `commons-io:2.16.1`, `org.slf4j:slf4j-api:2.0.16`, `ch.qos.logback:logback-classic:1.5.6`, `com.typesafe:config:1.4.3`, `com.fasterxml.jackson.core:jackson-databind` (via `jackson-bom:2.17.1`) | `SeaTunnelException` hierarchy; HOCON parsing/validation; logging bootstrap | `./mvnw -pl seatunnel-common,seatunnel-config -am test` |
| #3 | 3 API | `seatunnel-api` | *(none new — depends on `seatunnel-common` only)* | `SeaTunnelRow`, `CatalogTable`, `Option`/`Options`, `SeaTunnelSource`/`SourceReader`/`SplitEnumerator`, `SeaTunnelTransform`, `SeaTunnelSink`/`SinkWriter`/`SinkCommitter`, checkpoint/state contracts | `./mvnw -pl seatunnel-api -am test` |
| #4 | 4 Discovery + Formats | `seatunnel-plugin-discovery`, `seatunnel-formats` | JDK `ServiceLoader` (SPI), `jackson-databind`, `org.apache.commons:commons-csv:1.11.0`, `org.apache.avro:avro:1.12.0`, `org.apache.parquet:parquet-format-structures:1.15.0` | Plugin discovery SPI + classloader isolation; `SerializationSchema`/`DeserializationSchema` format SPI | `./mvnw -pl seatunnel-plugin-discovery,seatunnel-formats -am test` |
| #5 | 5 Runnable baseline | `seatunnel-core`, `seatunnel-cli`, `connector-fake`, `connector-console` | `info.picocli:picocli:4.7.6` | HOCON job-config schema; CLI contract (`--config`, `--mode local`); local runner | `./mvnw -pl seatunnel-cli -am verify` + E2E `FakeSource → ConsoleSink` job test |
| #6 | 6a DAG & plan | `seatunnel-engine` (`dag`, `plan` packages) | *(none new)* | Logical DAG, `PhysicalPlan`, `TaskGroup` generation | `./mvnw -pl seatunnel-engine -am test` |
| #7 | 6b Master/Worker | `seatunnel-engine` (`cluster`, `task` packages) | `com.hazelcast:hazelcast:5.5.0` (membership + remote transport), `io.micrometer:micrometer-core:1.13.4`; test-only `org.awaitility:awaitility:4.2.2`; Netty deferred until a distinct data-plane need | `TaskExecutionService`, member lifecycle, metrics | `./mvnw -pl seatunnel-engine -am verify` (embedded-Hazelcast integration tests) |
| #8 | 6c Splits + checkpoint | `seatunnel-engine` (`checkpoint` package) | *(Hazelcast `IMap` for state — no new dep)* | Split ownership ledger, replicated `CheckpointCoordinator` decisions, explicit retry/restore (not automatic ETL recovery) | `./mvnw -pl seatunnel-engine -am verify` (checkpoint + recovery test) |
| #9 | 6d Submit + REST | `seatunnel-engine`, `seatunnel-cli` (cluster mode) | `org.eclipse.jetty:jetty-server:11.0.20` | Job submission API, REST endpoints, status query | `./mvnw -pl seatunnel-engine -am verify` + cluster-submit E2E |
| #10 | 7a1 Field transforms + local chains | `seatunnel-transforms-v2`, discovery/core/CLI integration | JDK ServiceLoader and existing Typesafe Config 1.4.3; no new deps | FieldRename, Copy, ordered local chain lifecycle + E2E | `./mvnw -pl seatunnel-transforms-v2,seatunnel-core -am test` + CLI verify |
| #11 | 7a2 Literal strings | `seatunnel-transforms-v2` | JDK String.replace, String.split + Pattern.quote; no new dependencies | Literal replace, bounded split, local chain E2E | Affected tests + CLI verify (unrun while CI budget is fully allocated) |
| #12 | 7a3 JSON-path | `seatunnel-transforms-v2` | `com.jayway.jsonpath:json-path:2.9.0`, Jackson 2.17.1, managed json-smart/accessors-smart 2.5.2 | Schema-aware bounded JSON extraction (trusted queries) | Affected tests + CLI verify (unrun; no CI allocation) |
| #13 | 7a4 SQL filter | `seatunnel-transforms-v2` | JSqlParser4.9 + HSQLDB2.7.4 | Schema-bound SQL truth filtering | Merged; included in PR16 reactor verification |
| #14 | 7a5 SQL field projection | `seatunnel-transforms-v2` | Existing JSqlParser | Field selection/order/aliases | Merged; included in PR16 reactor verification |
| #15 | 7a6 SQL string expressions | `seatunnel-transforms-v2` | Existing parser/HSQLDB | Bounded string functions | Merged; included in PR16 reactor verification |
| #16 | Verification/logging fix | `seatunnel-cli`, CI | Existing Logback | stderr logs, JSON-only stdout | Full reactor verify passed at032f66f |
| #17 | 7a7 SQL LONG expressions | `seatunnel-transforms-v2` | Existing parser/HSQLDB | Bounded BIGINT arithmetic | Merged explicitly; local checks, new Maven gates unrun |
| #18 | 7a8 integer literals | `seatunnel-transforms-v2` | Existing parser/HSQLDB + JDK BigInteger | Signed decimal literals | Merged explicitly; local checks, new Maven gates unrun |
| #19 | 7a9 string/NULL literals | `seatunnel-transforms-v2` | Existing parser/HSQLDB | Plain SQL strings and NULL | Merged explicitly; local checks, new Maven gates unrun |
| #20 | 7a10 IN/BETWEEN | `seatunnel-transforms-v2` | Existing parser/HSQLDB | Bounded predicates | Merged explicitly; local checks, new Maven gates unrun |
| #21 | 7a11 LIKE/ESCAPE | `seatunnel-transforms-v2` | Existing parser/HSQLDB | Bound literal patterns, engine NULL semantics | Merged explicitly; local checks, new Maven gates unrun |
| #23–#25 | 7b advanced transforms + multi-input | API, transforms, core, CLI | Existing Guava/JDK/SPI | RowKindExtractor, FilterToken, bounded TableMerge UNION ALL | Implemented with supplemental checks; Maven/JUnit/installed CLI gates unrun |
| Optional | 7c Translation | `seatunnel-translation-flink`, `seatunnel-translation-spark` | Flink 1.19 / Spark 3.5 (pin at slice start) — **OPTIONAL** | Translation layer (only if Spark/Flink interop is in scope) | Deferred until scope is confirmed |
| Planned | 7d JDBC | `connector-jdbc` (source + sink) | HikariCP, PostgreSQL/MySQL JDBC drivers, Testcontainers (exact versions/license review at implementation entry) | `JdbcSource`/`JdbcSink` | Testcontainers E2E (real Postgres/MySQL) |
| Planned | 7e Kafka | `connector-kafka` (source + sink) | `org.apache.kafka:kafka-clients` (pin at implementation entry) | `KafkaSource`/`KafkaSink` | Testcontainers E2E (real Kafka) |
| Planned | 7f File | `connector-file` (local FS) | `org.apache.hadoop:hadoop-client` (pin at slice start) | `FileSource`/`FileSink` (CSV/JSON/Parquet/ORC/Text) | E2E on local FS |
| Planned | 7g Redis | `connector-redis` | `redis.clients:jedis` (pin at slice start) | `RedisSource`/`RedisSink` | Testcontainers E2E |
| Planned | 7h MongoDB | `connector-mongodb` | `org.mongodb:mongodb-driver-sync` (pin at slice start) | `MongoSource`/`MongoSink` | Testcontainers E2E |
| Planned | 7i HTTP | `connector-http` | `com.squareup.okhttp3:okhttp` (pin at slice start) | `HttpSource`/`HttpSink` | WireMock/Testcontainers E2E |
| Planned | 7j CDC | `connector-cdc` (Debezium) | `io.debezium:debezium-api` + `debezium-embedded` (pin at slice start) | `CdcSource` (MySQL/Postgres) | Testcontainers E2E |
| Planned | 7k Object storage | `connector-s3`, `connector-gcs`, `connector-azure` | AWS SDK v2 / Google Cloud Storage / Azure Storage (pin per PR) | Object-storage source/sink | LocalStack/Testcontainers E2E |
| Planned | 8 E2E harness | `seatunnel-e2e` | `org.awaitility:awaitility` (pin at slice start) | Shared E2E framework (job submit → assert output) | `./mvnw -pl seatunnel-e2e -am verify` |
| Planned | 9 Distribution | `seatunnel-dist`, Docker | `maven-assembly-plugin` / `maven-shade-plugin`, Docker | Distribution layout, Docker image, smoke test | `./mvnw -B -ntp verify` + image smoke test |

## Adopt-vs-build matrix (reuse-first)

| Problem | Adopted library | Custom glue only |
|---------|-----------------|------------------|
| Config parsing | Typesafe Config (HOCON) | Typed accessors + validation |
| CLI | Picocli | Command classes |
| JSON / serialization | Jackson | Format adapters |
| CSV / Avro / Parquet | Commons CSV / Avro / Parquet | Format adapters |
| Clustering, distributed maps, state | Hazelcast | Task/checkpoint services |
| Networking / metrics | Netty, Micrometer, SLF4J | Thin instrumentation |
| SQL parsing (transforms) | JSqlParser | Transform impls |
| JDBC / Kafka / CDC / cloud | HikariCP + drivers, Kafka Clients, Debezium Engine, AWS/GCS/Azure SDKs | Source/Sink adapters |
| Tests | JUnit 5, AssertJ, Testcontainers | E2E job configs |
| Build quality | Spotless, license plugin, Enforcer, JaCoCo | Root POM config (this PR) |

## PR #1 — Slice 1: Build & Repo Scaffold (this PR)

**Scope.** Root `pom.xml` (packaging `pom`, **no modules yet**), Maven Wrapper
(`mvnw`, `mvnw.cmd`, `.mvn/wrapper/` — wrapper jar auto-downloads from Maven
Central on first run, so no binaries are committed), `.gitignore`, Apache-2.0
`LICENSE`, GitHub Actions CI (`.github/workflows/ci.yml`: Temurin JDK 17 +
`./mvnw -B -ntp verify`), plus `README.md`, `CONTRIBUTING.md`, and this roadmap.

**Build contract locked by this PR.**

- Coordinates: `org.apache.seatunnel:seatunnel:3.0.0-SNAPSHOT`
  (mirrors upstream for fidelity; `3.0.0` distinguishes the clone from
  upstream `2.3.x`; not published to Maven Central).
- Toolchain: JDK 17 (`maven.compiler.release=17`), Maven ≥ 3.9 (Enforcer).
- Centralized `dependencyManagement`: BOM imports (Jackson, JUnit, Testcontainers,
  Netty, Micrometer) + direct pins for Slices 2–6 libraries. Connector SDK
  versions are deliberately **not** pinned yet — each connector PR pins its own
  SDKs to avoid stale versions.
- Quality gates bound to `verify`: Spotless `check` (google-java-format for
  Java only — no SortPom `<pom/>` step yet, since SortPom's 2-space +
  recommended-sort-order rules would fight this POM's 4-space,
  comment-grouped layout; POMs follow the 4-space convention by review),
  `license:check` (Apache-2.0 headers), Enforcer (Maven/Java versions,
  dependency convergence), JaCoCo report. `license:check` uses
  `failIfMissing=false` until Slice 2 creates `src/main/java` (flip to `true` then).
- CI runs exactly `./mvnw -B -ntp verify` on every push/PR to `main`.

**Out of scope (deferred).** All production modules (Slices 2+); connector SDK
pins; coverage thresholds (JaCoCo report only for now); release/distribution
profiles (PR #22).

**Verification.**

```bash
./mvnw -B -ntp verify          # full gate (what CI runs)
./mvnw -B -ntp validate        # fast structural check
./mvnw spotless:check          # formatting gate only
```

> **Sandbox note.** The agent sandbox has no JDK/Maven and its egress proxy
> blocks Maven Central, so `./mvnw verify` cannot execute locally here.
> The POM was validated as well-formed XML and the wrapper scripts were
> syntax-checked; the authoritative build runs in GitHub Actions CI.

## Handoff to Slice 2 (next PR)

- **PR #2 — Slice 2: `seatunnel-common` + `seatunnel-config`.** Add both modules
  to the root `<modules>`; switch `license:check` to `<licenseSets>` with
  `<roots>` (`src/main/java`, `src/test/java`) and `failIfMissing=true`;
  run `./mvnw license:format` once to normalize headers.
- **Adopt:** Guava, Commons Lang3/Collections4/IO, SLF4J + Logback, Typesafe
  Config, Jackson (versions already pinned in the root POM).
- **Lock:** `SeaTunnelException` hierarchy, HOCON parsing/validation, logging
  bootstrap. **Verify:** `./mvnw -pl seatunnel-common,seatunnel-config -am test`.

### Slice 7a4: predicate filtering

PR #12 (7a3 JSON-path) merged explicitly by request; Maven gates remain unverified.
7a4 adds `sql-filter` only: JSqlParser 4.9 (Apache-2.0 option) + HSQLDB 2.7.4
(permissive), transforms module, schema-preserving filter, parameterized private SQL.
See [specification](SLICE-7A4.md). No API engine coupling. PR #13 merged by explicit
request; its unrun Maven gates were not waived for future slices.
Affected Maven tests, CLI E2E and full reactor gates remain unrun; no CI reservation.


### Slice 7a5: field-only SQL projection

`sql-project` parses a SELECT list with existing JSqlParser 4.9 (Apache-2.0 option),
compiles an allowlisted AST and reuses the existing positional mapping implementation.
No new dependency, SQL evaluator or engine/API coupling. Select/order/drop/explicit
alias only; computed expressions remain deferred. [Specification](SLICE-7A5.md).
62 supplemental assertions and the actual LocalRunner -> FakeSource -> sql-filter ->
sql-project -> ConsoleSink example passed. Pinned Maven/JUnit and packaged CLI E2E
remain unrun. Zero Actions, `[skip ci]` draft PR; no automatic merge authorization.


### Slice 7a6: bounded SQL string expressions

PR #14 / 7a5 explicitly merged by request, without a Maven verification claim.
7a6 adopts existing pinned JSqlParser 4.9/HSQLDB 2.7.4 for UPPER/LOWER/CONCAT/COALESCE
of STRING fields only, appending one nullable STRING. Share the existing filter JDBC
row-query implementation; no new dependency or API/engine coupling. General computed
projection remains deferred. [Specification](SLICE-7A6.md).
59 supplemental parser/JDBC assertions and three actual LocalRunner examples passed;
Maven/JUnit, packaged CLI and full reactor remain unrun. Draft, [skip ci], zero Actions.


### Verification checkpoint and Slice 7a7

PR #16 fixed CLI log routing (stderr logs, JSON-only stdout), passed full Maven
reactor verify at032f66f in run46033295536 (3m1s), and merged explicitly by request.
This verifies the prior slices through7a6, not future changes.

7a7 adds `sql-long`: INT/LONG field arithmetic with +,-,* and unary signs/parentheses,
via pinned JSqlParser/HSQLDB; each reference promotes to BIGINT. Reuse SqlRowQuery,
share append row/schema ownership with sql-string; no dependency/API/engine changes.
[Specification](SLICE-7A7.md). Division/literals/decimals/general SQL remain deferred.
39 new integer smoke assertions +59 prior string/filter regressions and four real
LocalRunner SQL examples passed. New Maven/JUnit/packaged CLI gates remain pending.
Zero Actions, [skip ci] draft; no automatic merge or additional CI authorization.


### Slice 7a8: signed decimal integer literals

PR17 /7a7 explicitly merged with supplemental checks; new Maven gates remain unrun.
7a8 extends sql-long only: decimal BIGINT literals, direct signed64 boundaries and
constant expressions. Reuse JSqlParser/HSQLDB, JDK BigInteger.longValueExact validates
literal range, SQL engine still owns arithmetic and overflow. No new plugin/dependency/
API/engine changes. [Specification](SLICE-7A8.md).
30 literal checks +37 updated integer +59 string/filter regressions and five actual
LocalRunner SQL examples passed. JUnit/CLI/full Maven gates remain unrun. Zero Actions,
[skip ci] draft; no automatic merge or expanded SQL scope authorized.


### Slice 7a9: standard string and SQL NULL literals

PR18 /7a8 explicitly merged with local supplemental evidence only. 7a9 extends
sql-string's existing allowlist with plain StringValue and NullValue nodes, using
JSqlParser4.9 parsing/serialization and HSQLDB2.7.4 typed VARCHAR evaluation. Backslash
escaping explicitly disabled; prefixes reject, lexer errors normalized before open.
No new plugin/dependency/API/engine changes. [Specification](SLICE-7A9.md).
30 literal assertions +124 regressions (57 string/filter,37 integer,30 integer literal)
and six actual LocalRunner SQL examples passed. New JUnit/CLI/full Maven gates remain
unrun. Zero Actions, [skip ci] draft; no automatic merge or expanded dialect permission.


### Slice 7a10: bounded IN and BETWEEN predicates

PR19 /7a9 explicitly merged with supplemental evidence only. 7a10 extends sql-filter
with flat scalar IN/NOT IN lists (1..64) and BETWEEN/NOT BETWEEN. Reuse existing pinned
JSqlParser/HSQLDB and JDBC adapter, no new dependencies/plugin/API/engine coupling.
AST validates structure/identifiers; engine owns membership/range/NULL/negation behavior.
Reject subqueries/tuples/decorations, count list/sign nodes, normalize lexer errors.
[Specification](SLICE-7A10.md). 47 new assertions +154 regressions and seven actual
LocalRunner SQL examples passed. New JUnit/packaged CLI/full Maven gates remain unrun.
Zero Actions, [skip ci] draft; no automatic merge or broader SQL scope authorization.


### Slice 7a11: bounded LIKE / NOT LIKE / ESCAPE

PR20 /7a10 already merged explicitly. 7a11 adds standard LIKE on STRING scalars with
bounded literal/NULL patterns and optional one-character ESCAPE. Existing JSqlParser/
HSQLDB own syntax/evaluation; no new library/plugin/API/core/engine coupling. Reject
regex dialects, dynamic patterns, oversized/prefixed literals and invalid escapes.
A pinned-source HSQLDB nullable-derived-row constant-pattern optimization quirk was
found locally: bind pattern/escape as typed JDBC parameters instead of adding custom
NULL/matching evaluation. [Specification/probe](SLICE-7A11.md).
54 new assertions +201 regressions and eight actual LocalRunner SQL examples passed.
New JUnit/packaged CLI/full Maven gates remain unrun; zero Actions, [skip ci] draft.


### Remaining Stage7 — authorized full scope, planning checkpoint

PR21 /7a11 explicitly merged. User selected all remaining Stage7, not merely remaining
SQL functions. [Completion program](STAGE-7-COMPLETION.md) defines the complete mandatory
matrix, runtime blockers, dependency/license adoption and verification criteria.
No empty connector stubs or falsely green mock E2E. This planning change implements no
new connector or advanced transform. Next implementation starts with7b prerequisites/
contracts, then cohesive connector PRs, JDBC first. Optional translation stays deferred.


### Slice7b1: native row-kind metadata and extractor

Planning PR22 explicitly merged. First advanced prerequisite adds engine-independent
RowKind enum/SeaTunnelRow metadata (legacy constructors INSERT, old Java streams INSERT),
immutable kind/field copies, metadata-aware equality and preservation through every
existing transform. row-kind-extractor appends actual native short code, not payload
inference. [Specification](SLICE-7B1.md). No new dependency/API engine coupling.
123 local API/legacy/operator assertions +255 regressions and nine actual LocalRunner
examples passed; JUnit/Maven/packaged CLI remain unrun. Zero Actions. User explicitly
authorized merge22, next implementation, then merge that next PR; exception applies
only to this deliverable. Remaining advanced transforms/connectors still outstanding.


### Slice7b2: exact token filtering

PR23 /7b1 merged explicitly. filter-token adopts centrally pinned Guava33.2.1-jre
Splitter with literal delimiters and explicit ANY/ALL set membership; no custom token
parser/regex engine. Schema/original-row identity and all native kinds preserved. Strict
options/bounds/null/lifecycle contracts; lazy iteration, bounded match set, no split array.
[Specification](SLICE-7B2.md). Direct Guava dependency in transforms-v2 uses existing
central version; no new root pin/API/core/engine coupling.
43 supplemental token assertions +380 regressions and ten actual LocalRunner examples
passed. Guava/Jackson temporary import relinks are not pinned Maven proof. Added JUnit/
packaged CLI/full reactor remain unrun. Zero Actions; user explicitly authorized next
implementation then merge for this PR only. TableMerge/runtime/connectors remain pending.


### Slice7b3: bounded TableMerge / remaining named 7b implementation

PR24 /7b2 merged explicitly. User requested "next then merge tackle all of 7b now".
[Specification](SLICE-7B3.md) locks schema-aligned bounded UNION ALL (not relational join).
Existing core lifecycle and SPI reused: strict source/sources config, 2..32 inputs,
serial drain, one shared transform chain/writer/final flush, preflight schema checks,
per-input resource cleanup, no hidden queues/new dependencies/API engine imports.
207 executable contracts and eleven actual LocalRunner examples passed. Added JUnit/
installed CLI tests and Maven/full reactor gates remain unrun. Zero Actions. All named
7b features implemented, not fully verified. Next is JDBC; streaming prerequisites
still required before unbounded connectors. This merge authorization covers 7b, not
new CI spend or connector merges.
