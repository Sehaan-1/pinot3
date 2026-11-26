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
| 6a | `seatunnel-engine`: logical DAG + physical task planning | In progress |
| 6b–6d | Zeta: Master/Worker → checkpoint → submit/REST | ⬜ |
| 7+ | Transforms, translation (optional), one connector per PR + Testcontainers E2E | ⬜ |

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
