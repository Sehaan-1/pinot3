# Slice Roadmap — SeaTunnel Clone

> **How to read this doc.** Each row is exactly one PR. A PR merges only when
> its verification command is green in CI. Downstream PRs never start before
> their upstream contracts are merged. Versions below are pinned in the root
> POM's `dependencyManagement` at the slice that first adopts them; connector
> SDK versions are pinned in their own PR (not upfront) to avoid stale pins.

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
            │              └─ PR #10–#11  Transforms v2
            │                   └─ PR #13+  Connectors (one per PR, Testcontainers E2E)
            │                        └─ PR #21  E2E harness   └─ PR #22  Distribution
            └─ PR #12  Translation (Spark/Flink) — OPTIONAL, deferrable
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
| Next | 7a2 Remaining core transforms | `seatunnel-transforms-v2` | Adopt/pin JSqlParser for SQL/filter; dedicated JSON-path library before implementation | SQL, Filter, Replace, Split, JsonPath — deferred from original broad 7a | Affected tests + real pipeline E2E |
| #11 | 7b Transforms (advanced) | `seatunnel-transforms-v2` | *(none new)* | TableMerge, RowKindExtractor, FilterToken, … | `./mvnw -pl seatunnel-transforms-v2 -am test` |
| #12 | 7c Translation | `seatunnel-translation-flink`, `seatunnel-translation-spark` | Flink 1.19 / Spark 3.5 (pin at slice start) — **OPTIONAL** | Translation layer (only if Spark/Flink interop is in scope) | Deferred until scope is confirmed |
| #13 | 7d JDBC | `connector-jdbc` (source + sink) | `com.zaxxer:HikariCP:5.1.0`, PostgreSQL/MySQL JDBC drivers, `org.testcontainers:testcontainers-bom:1.20.0` | `JdbcSource`/`JdbcSink` | Testcontainers E2E (real Postgres/MySQL) |
| #14 | 7e Kafka | `connector-kafka` (source + sink) | `org.apache.kafka:kafka-clients:3.9.0` | `KafkaSource`/`KafkaSink` | Testcontainers E2E (real Kafka) |
| #15 | 7f File | `connector-file` (local FS) | `org.apache.hadoop:hadoop-client` (pin at slice start) | `FileSource`/`FileSink` (CSV/JSON/Parquet/ORC/Text) | E2E on local FS |
| #16 | 7g Redis | `connector-redis` | `redis.clients:jedis` (pin at slice start) | `RedisSource`/`RedisSink` | Testcontainers E2E |
| #17 | 7h MongoDB | `connector-mongodb` | `org.mongodb:mongodb-driver-sync` (pin at slice start) | `MongoSource`/`MongoSink` | Testcontainers E2E |
| #18 | 7i HTTP | `connector-http` | `com.squareup.okhttp3:okhttp` (pin at slice start) | `HttpSource`/`HttpSink` | WireMock/Testcontainers E2E |
| #19 | 7j CDC | `connector-cdc` (Debezium) | `io.debezium:debezium-api` + `debezium-embedded` (pin at slice start) | `CdcSource` (MySQL/Postgres) | Testcontainers E2E |
| #20 | 7k Object storage | `connector-s3`, `connector-gcs`, `connector-azure` | AWS SDK v2 / Google Cloud Storage / Azure Storage (pin per PR) | Object-storage source/sink | LocalStack/Testcontainers E2E |
| #21 | 8 E2E harness | `seatunnel-e2e` | `org.awaitility:awaitility` (pin at slice start) | Shared E2E framework (job submit → assert output) | `./mvnw -pl seatunnel-e2e -am verify` |
| #22 | 9 Distribution | `seatunnel-dist`, Docker | `maven-assembly-plugin` / `maven-shade-plugin`, Docker | Distribution layout, Docker image, smoke test | `./mvnw -B -ntp verify` + image smoke test |

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
