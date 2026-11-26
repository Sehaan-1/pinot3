# Slice 4 — Plugin discovery and format adapters

## Specification and dependency plan

PR title: `[Slice 4] Isolated ServiceLoader discovery and library-backed formats`.

- `seatunnel-plugin-discovery`: `org.apache.seatunnel.plugin`, depends on
  `seatunnel-api:3.0.0-SNAPSHOT`. No engine/config/format imports.
- `seatunnel-formats`: `org.apache.seatunnel.format`, depends on
  `seatunnel-api:3.0.0-SNAPSHOT`, not discovery. No engine/connector imports.
- All internal module versions inherit `${project.version}`. Dependency direction
  remains API -> common, discovery/formats -> API; no cycles.

Build/adopt: use JDK ServiceLoader and URLClassLoader, wrapping only bundle loading,
identity checks and lifecycle. Child-first implementation loading isolates bundle
libraries; JDK, SeaTunnel API/common/plugin SPI and SLF4J types remain parent-owned.
Each bundle is an explicit list of JARs, including its dependencies. No custom JAR
scanner or dependency resolver. Isolation is dependency isolation, NOT a security
sandbox: only trusted plugin code may be loaded.

Format SPI consists of `SerializationSchema<T>` and `DeserializationSchema<T>`.
Adopt these Apache-2.0 libraries rather than implementing parsers or wire protocols:

| Library | Pinned coordinate | Adapter |
|---|---|---|
| Jackson | `com.fasterxml.jackson.core:jackson-databind:2.17.1` (existing BOM) | Typed single JSON value |
| Commons CSV | `org.apache.commons:commons-csv:1.11.0` | One RFC4180 record of strings |
| Avro | `org.apache.avro:avro:1.12.0` | Schema-bound binary GenericRecord datum |
| Parquet | `org.apache.parquet:parquet-format-structures:1.15.0` | Thrift FileMetaData bytes only |

Coordinate correction: upstream parquet-java tag `apache-parquet-1.15.0` publishes
`parquet-format-structures:1.15.0`; `parquet-format` uses a different release series.
Correct the roadmap/root management instead of guessing a nonexistent version.
Parquet metadata is not a Parquet row/file codec. Hadoop/filesystem/container I/O,
Parquet pages, Avro object containers and schema registry integration are out of
scope. JSON/CSV/Avro payloads do not silently become SeaTunnelRows: schema-to-row
mapping belongs in later connectors. No dummy row conversion is introduced.

Align transitive `commons-codec:commons-codec:1.17.0` in root dependency management:
CSV requests 1.16.1, while Avro/compress requests 1.17.0. Keep convergence enforced.

Tests use existing JUnit 5.11.0 and AssertJ 3.26.3. No mocking framework is needed.

## Contracts

- Plugin identifiers are unique lowercase IDs; provider lookup fails clearly for
  missing/duplicate/invalid providers. Discovery order is sorted by identifier.
- Discovery changes the thread context loader only for provider construction and
  restores it on success/failure. Callers must manage TCCL when invoking libraries
  that require it. Bundle close releases JAR handles, is idempotent, and prevents
  new lookup. Consumers must stop using returned providers before closing bundles;
  providers own their runtime resources, not discovery.
- One discovery instance owns one bundle/classloader; dependency JARs may be shared
  within that bundle, not across independently loaded bundles. API copies in plugin
  JARs cannot override host contract types. Parent service descriptors do not leak
  into explicit bundle discovery.
- Formats operate on one non-null value/message. Invalid data raises a checked
  FormatException with its cause. Codec instances are safe for concurrent calls;
  Avro creates per-call readers/writers, Jackson configuration is private and fixed.
- CSV preserves empty strings, quoting and embedded newlines; null fields and
  multiple input records are rejected. Null is not conflated with an empty field.
- Avro callers must use compatible records; the constructor snapshots schema JSON.
  Parquet callers own mutable metadata values and must not mutate during encoding.
- Input size limits and backpressure belong to the calling transport/runtime.

## Verification

```sh
./mvnw -pl seatunnel-plugin-discovery,seatunnel-formats -am test
./mvnw -B -ntp verify
```

Tests build real JARs with the JDK compiler and exercise independent bundle class
identities, SPI sharing, service registration errors, TCCL restoration and close.
Formats have real library round-trips, malformed input and concurrent reuse tests.
No engine or network services are required. Executable checks run in GitHub Actions
because the agent sandbox has no JDK or Maven Central access.

## Three-line handoff

Next: Slice 5 adds core/CLI plus FakeSource -> ConsoleSink as the first runnable job.
Adopt Picocli 4.7.6; wire config, these SPIs and deterministic resource cleanup.
Wait for this PR to be reviewed, green and merged before starting Slice 5.
