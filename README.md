# SeaTunnel (Clone)

A clean, production-grade clone of [Apache SeaTunnel](https://github.com/apache/seatunnel),
built **slice by slice, one PR per slice**, so `main` stays green, compilable, and
testable after every merge.

SeaTunnel is a distributed data-integration engine (the **Zeta** engine) for batch
and streaming ETL: sources read data, transforms reshape it, and sinks write it —
all driven by declarative HOCON job configs and a plugin architecture.

## Status

| Slice | Scope | Status |
|-------|-------|--------|
| 1 | Build & repo scaffold (root POM, Maven wrapper, Spotless/license, CI) | ✅ Merged |
| 2 | `seatunnel-common` + `seatunnel-config` | ✅ Merged |
| 3 | `seatunnel-api` (engine-independent contracts) | ✅ Merged |
| 4 | `seatunnel-plugin-discovery` + `seatunnel-formats` | ✅ Merged |
| 5 | `seatunnel-core` + `seatunnel-cli` + FakeSource + ConsoleSink (runnable baseline) | ✅ Merged |
| 6a | `seatunnel-engine`: logical DAG + physical task planning | ✅ Merged |
| 6b | Embedded Master/Worker and bounded remote tasks | ✅ Merged |
| 6c | Replicated checkpoint decisions, split state and explicit recovery | ✅ Merged |
| 6d | Installed job submission REST + HTTP cluster client | ✅ Merged by request; final gates unverified |
| 7a1 | Field rename/copy + ordered local transform chains | ✅ Merged; full reactor run timed out |
| 7a2 | Literal string replace/split | ✅ Merged by request; local checks only |
| 7a3 | Jayway JSON-path extraction | Draft; supplemental checks only |
| 7a4+ | SQL/filter, advanced transforms, connectors | ⬜ |

Full plan: [docs/ROADMAP.md](docs/ROADMAP.md). Workflow: [CONTRIBUTING.md](CONTRIBUTING.md).

## Quickstart

Prerequisite: **JDK 17** (Temurin recommended). No system Maven needed — the
wrapper (`mvnw`) bootstraps Maven 3.9.9 on first run.

```bash
# Build, test, and verify (Spotless + license + JaCoCo) — same command CI runs
./mvnw -B -ntp verify

# Format code (run before every commit)
./mvnw spotless:apply

# Build/test a single module and its dependencies (from Slice 2 onward)
./mvnw -pl <module> -am test
```

## Run the local baseline

```bash
./mvnw -pl seatunnel-cli -am verify
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar \
  --config examples/fake-to-console.conf --mode local
```

The example emits five JSON rows, IDs 10–14. This slice supports bounded local
execution with parallelism 1 only. Console output is non-transactional; no
exactly-once or distributed-engine support is claimed.
See [the Slice 5 specification](docs/SLICE-5.md) for config and lifecycle contracts.

## Repository layout (planned)

```
pom.xml                  # Root aggregator: BOM, plugin management, quality gates
mvnw / mvnw.cmd          # Maven Wrapper (Maven 3.9.9)
.mvn/wrapper/            # Wrapper config (jar auto-downloaded on first run)
.github/workflows/ci.yml # CI: JDK 17 + ./mvnw -B -ntp verify
docs/ROADMAP.md          # Slice-by-slice PR roadmap (PR #1 … PR #N)
seatunnel-common/        # Exceptions, utilities, logging/metrics glue (Slice 2)
seatunnel-config/        # HOCON config parsing (Slice 2)
seatunnel-api/           # Engine-independent SPI contracts (Slice 3)
seatunnel-plugin-discovery/ # ServiceLoader plugin loading (Slice 4)
seatunnel-formats/       # JSON/CSV/Avro/Parquet formats (Slice 4)
seatunnel-core/          # Job parsing & local runner (Slice 5)
seatunnel-cli/           # Picocli CLI (Slice 5)
seatunnel-engine/        # Zeta engine: DAG, Hazelcast cluster, checkpoint (Slice 6)
seatunnel-transforms-v2/ # Transform plugins (Slice 7)
seatunnel-connectors-v2/ # Source/Sink plugins, one per PR (Slice 7+)
seatunnel-e2e/           # Testcontainers end-to-end tests (Slice 7+)
```

## Design principles

1. **Reuse first.** Every non-trivial problem is delegated to a battle-tested
   library (Typesafe Config, Jackson, Hazelcast, Kafka Clients, Debezium, …);
   this repo writes only thin adapters. See the adopt-vs-build matrix in
   [docs/ROADMAP.md](docs/ROADMAP.md).
2. **One slice = one PR.** No PR bundles multiple layers or unrelated
   connectors. Downstream modules never land before upstream contracts are
   merged and tested.
3. **Layering is enforced.** `seatunnel-api` has zero engine/connector imports;
   the engine depends on the API, never the reverse.

## License

Apache License, Version 2.0 — see [LICENSE](LICENSE). Coordinates mirror
upstream (`org.apache.seatunnel:seatunnel:3.0.0-SNAPSHOT`) for fidelity;
this clone is not published to Maven Central.

## Installed job cluster client (Slice 6d)

An embedder installs trusted `JobDefinition` factories in `JobSubmissionService`,
then starts `JobRestServer(service, bindAddress, port)` on the oldest engine member.
Factories return physical plans and task bindings whose classes exist on all workers.
The library does **not** expose generic connector HOCON submission or upload code.
No server launcher or automatically wired distributed ETL is claimed.

```sh
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar cluster submit \
  --endpoint http://127.0.0.1:8080 --definition installed-job --parameters parameters.json
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar cluster status \
  --endpoint http://127.0.0.1:8080 --job <job-uuid>
# cluster cancel / cluster delete use the same endpoint and job options
```

`parameters.json` is a JSON object of strings. Definition IDs are chosen by the
embedder; `installed-job` is illustrative, not a built-in production job. Poll
status to observe failures/cancel siblings. Delete terminal records to release
retention capacity. Do not blindly retry an uncertain POST. Private trusted
networks only: this baseline has no TLS/authentication. See `docs/SLICE-6D.md`.
Actions usage is constrained project-wide; consult `docs/CI-BUDGET.md` before pushing.

## Local field transforms (Slice 7a1)

```sh
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar \
  --config examples/fake-rename-copy-console.conf --mode local
```

The example emits `[10,"row-10",10]` and `[11,"row-11",11]`. The optional ordered
`transforms` list applies `field-rename` (`fields={old=new}`) then `field-copy`
(`fields={new=existing}`). Copy targets append in lexical order. Stages receive
the prior stage's schema; the sink receives the final schema. At most 32 stages,
128 mappings per field transform. Names are case-sensitive; collisions and unknown
fields fail before source reading. Copies are shallow, matching `SeaTunnelRow`.
This remains bounded local execution, not distributed connector ETL. SQL/filter,
Advanced transforms and SQL/filter remain deferred. JSON-path is introduced in 7a3.

## Literal string transforms (Slice 7a2)

```sh
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar \
  --config examples/fake-replace-split-console.conf --mode local
```

`string-replace` uses `field`, `search`, `replacement`: all literal strings, not
regex. `string-split` uses `field`, `separator`, `fields=[newA,newB]`: append nullable
STRING columns, splitting at most N-1 times for N output names. The last field
retains the remainder, trailing empty strings stay empty, missing tokens become
null. Null source values yield null outputs. Input field must have STRING type;
new fields cannot collide. The example emits `[10,"row|10","row","10"]` and
`[11,"row|11","row","11"]`. See `docs/SLICE-7A2.md` for lifecycle and bounds.
This slice was merged by explicit request with local smoke checks only. Maven/CI
gates remain unrun; the merge is not evidence of a green build.

## JSON-path extraction (Slice 7a3)

```sh
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar \
  --config examples/fake-json-path-console.conf --mode local
```

`json-path` accepts `field`, `path`, `target`. It appends one nullable STRING column:
strings remain strings, objects/arrays/numbers/booleans become compact JSON text,
and missing definite paths or JSON/source null become null. Indefinite queries
return their array result as JSON text (including `[]`). Queries use the Jayway
dialect. The example appends `Ada` from `$.user.name`.

Input must be valid JSON: duplicate keys, trailing tokens and depth over 64 are
rejected. Input limit is 1,048,576 UTF-16 code units; path limit is 4096 characters.
These are parsing bounds, not a CPU sandbox: use trusted query configurations only.
Jackson providers are per operator; global JsonPath defaults are untouched. See
`docs/SLICE-7A3.md`. Maven/JUnit/packaged CLI verification is unrun; draft only.
