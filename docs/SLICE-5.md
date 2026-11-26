# Slice 5 — Minimal runnable local baseline

## Specification and dependency plan

PR title: `[Slice 5] Local FakeSource to ConsoleSink pipeline and CLI`.

- `seatunnel-core` (`org.apache.seatunnel.core`): strict job-config validation and
  single-reader bounded execution. Depends on config and plugin-discovery/API.
- `connector-fake` (`org.apache.seatunnel.connector.fake`): deterministic bounded
  range source, split enumeration, detached reader/enumerator snapshots.
- `connector-console` (`org.apache.seatunnel.connector.console`): caller-owned Writer,
  JSON array per row, explicit flush and non-transactional semantics.
- Both connectors depend on discovery/API; console also uses formats. Neither
  imports core/CLI/engine. Discovery adds SourcePlugin/SinkPlugin factories and a
  minimal output context using JDK Writer; no engine-specific contract types.
- `seatunnel-cli` (`org.apache.seatunnel.cli`): depends on core and the two registered
  connectors. Tests run the actual packaged command, not mocked operators.

Build vs adopt: reuse Typesafe Config 1.4.3 for HOCON/substitution, the existing
Jackson adapter for JSON, and ServiceLoader for classpath factories. Implement
only pipeline orchestration and connector-specific range/output behavior. No new
parser, scheduler, protocol or distributed machinery. CLI adopts
`info.picocli:picocli:4.7.6`; executable packaging adopts
`org.apache.maven.plugins:maven-shade-plugin:3.6.0` with ServiceLoader metadata
merging. Tests use JUnit 5.11.0, AssertJ 3.26.3, Failsafe 3.2.5 and the JDK process API.
All adopted libraries/tooling have permissive licenses and versions are centralized.

## Config and execution contracts

```hocon
env { parallelism = 1 }
source {
  plugin = fake
  options { rows = 5, start = 10, split-size = 2 }
}
sink { plugin = console }
```

Unknown section/connector options, absent sections, invalid IDs, unresolved values,
wrong numeric types, negative rows, nonpositive split size and parallelism != 1
are rejected. Fake options are 32-bit integers; emitted IDs are LONG and addition
uses long arithmetic. `rows` defaults to 10, `start` to 0, `split-size` to 1000.
Console accepts no options. Source rows are `(id LONG NOT NULL, name STRING NOT NULL)`
with names `row-<id>`. Zero rows is a successful empty job.

`--config <file> --mode local` runs to bounded end-of-input. Mode defaults to local;
cluster/streaming/parallel execution are explicitly unsupported, not silently
emulated. stdout contains only JSON arrays, one per row. Help/version exit 0,
invalid CLI syntax/mode exits 2, config/plugin/runtime failure exits 1. Diagnostics
use stderr and do not dump the config. The CLI never closes caller-owned streams.

The runner creates actual readers, enumerators and writers, routes assignments
through the existing SPI, backs off briefly on idle polls, honors interruption,
validates output arity and flushes on success. Try-with-resources closes all created
resources in reverse order, retaining suppressed cleanup failures. Transactional
sinks and unbounded sources are rejected; there is no exactly-once claim. Discovery
constructors must be side-effect-free; operators own runtime resources. Classpath
discovery shares validation with explicit isolated bundles and does not own/close
the application classloader.

FakeSource supports snapshot/restore of remaining ranges, without introducing a
checkpoint coordinator. Notifications validate IDs but require no external commit:
the source is deterministic and owns no external resources. Console snapshots
record a count only and cannot roll back already printed rows. Its commit list is
empty and no committer is offered. Replaying a snapshot can print duplicates.

## Out of scope

Zeta/DAG, distributed scheduling, REST, remote plugin directories, transforms,
streaming, periodic checkpoints/recovery coordination and transactional output.
No Testcontainers service is necessary for in-memory fake and console connectors.

## Verification gate

```sh
./mvnw -pl connector-fake,connector-console,seatunnel-core -am test
./mvnw -pl seatunnel-cli -am verify
./mvnw -B -ntp verify
java -jar seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar \
  --config examples/fake-to-console.conf --mode local
```

Unit tests cover config, factory validation, split/reader restore, output failures,
non-ownership of output and runner cleanup/error/interrupt paths. Failsafe E2E tests
fork the packaged CLI JAR and check exact rows, empty input, help, invalid config,
unknown plugins and nonzero exits. A Linux `/dev/full` subprocess test verifies
that Java PrintStream errors cannot silently produce a success exit; it is skipped
on systems without that device. The sandbox has no JDK/Maven Central access;
executable verification is performed on JDK 17 GitHub Actions.

## Three-line handoff

Next: Slice 6a adds engine DAG/physical-plan contracts, not a full cluster in one PR.
Keep API free of engine imports and reuse this local baseline as a regression gate.
Start only after this runnable slice is green, reviewed and merged.
