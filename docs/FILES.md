# Local file connector — Slice7f

`file` is a bounded, single-reader source and INSERT-only single-writer sink for
trusted local files. Factories validate configuration without opening files or even
requiring the configured root to exist. Runtime owns file/parser/vector resources.

## Run

Create `/tmp/seatunnel-files`, then run the installed CLI with
`examples/file/write-json.conf` and `examples/file/read-json.conf`. Each of CSV,
JSON, text, Parquet and ORC has a matching write/read pair. Targets must not exist.
This is bounded LocalRunner integration, **not streaming/checkpoint/exactly-once**.

## Options and schema

Shared: `root` (absolute normalized directory), `format` (csv/json/text/parquet/orc),
`max-file-bytes` default67108864 (1..1073741824), `max-row-bytes` default1048576
(1..16777216). Source: `paths` (1..128 unique explicit relative file names), `schema`
(list of `{name,type,nullable}` objects). Sink: `path` (one relative output file);
schema comes from the pipeline. Unknown options are rejected. Max128 columns;
names match `[A-Za-z_][A-Za-z0-9_]{0,127}`. Primitive BOOLEAN/INT/LONG/FLOAT/DOUBLE/
STRING/BYTES only. Decimal, temporal, nested, partitioned datasets and precision/
timezone inference are intentionally unsupported. No implicit casts in sink rows;
source text-to-type conversion is strict, nullability enforced, finite floats only.
Columnar schemas must match configured names/order/types (and Parquet repetition).
ORC does not encode requiredness; configured nullability is checked per decoded row.

| Format | Framing / NULL / bytes |
|---|---|
| CSV | Commons CSV RFC4180, UTF8, no header, comma delimiter, quoted multiline fields. `\N` is reserved for NULL, cannot represent that literal string. Empty string is distinct. Bytes base64. |
| JSON | Jackson, one schema-ordered array per UTF8 line, strict trailing-token check; null is JSON null, bytes base64. Not JSON object files or whole-file arrays. |
| text | One non-null STRING column, UTF8 lines. Sink rejects CR/LF inside fields; reader accepts LF/CRLF/CR terminators. Empty line means empty string. |
| Parquet | Apache Avro-backed primitive records, no logical/nested types, uncompressed output. No custom codec. |
| ORC | Apache ORC primitive struct/vector batches, uncompressed output. No custom codec. |

Text output uses the platform line separator; runtime is JDK17. Malformed UTF8 fails
instead of replacement decoding. Source emits one row per poll. A file is one split,
not byte ranges, preserving CSV multiline and columnar boundaries. List order is
preserved. File size/mtime/fileKey are captured by the enumerator and checked before
open and at EOF; this detects ordinary mutation, **not a cryptographic immutable
snapshot**. Inputs must remain unchanged throughout execution.

## Paths, limits and ownership

Parents/root must already exist. No URI, glob, directory listing, overwrite, append,
auto-create parents or remote filesystem. Reject absolute child paths, dot/dotdot,
noncanonical child separators, symlinks in root/parents/leaves and nonregular source
files. Inputs/outputs require exclusive ownership of a **trusted directory tree**.
Checks are best-effort against accidents, not a hostile concurrent symlink/hardlink
race sandbox. Do not run against directories writable by untrusted users. Hardlinks
already in source files are not rejected. No file locks or fencing.

Limits check file size, logical combined row payload and staged bytes; buffered library
pages/vectors may exceed a byte limit before flush. Compressed inputs/parser headers
can cause allocations beyond limits, so do not accept untrusted files. Lines/CSV records
are parsed by established libraries; there is no hard streaming allocation cap inside
a record. Local filesystem IO cannot promise a wall-clock deadline or cancellable
blocked device call. Interrupts are checked between rows/files and before publication.

Sink creates a unique `.seatunnel-*.stage` file beside the target. `flush(false)` asks
the library to flush (Parquet may retain pages); it never publishes. `flush(true)` closes
the footer/stream, checks size, forces stage data, then creates a hard link at the target
and removes staging. Link creation is atomic and fails if any target already exists;
there is **no overwrite or unsafe fallback**. Filesystems lacking hardlink support fail.
Before publication, error/close discards staging. After publication, close never deletes
the target. Empty output still publishes a valid empty file. Repeat terminal flush is
idempotent; writes after finalization fail. This is not directory fsync, crash durability,
a distributed transaction or a recoverable commit handle. Process death can leave
staging files; only clean orphan stages when no writer is active. Deletion/close errors
are best-effort and may leave artifacts. A failure after hardlink creation may leave a
complete target even when the job reports failure; inspect before explicitly rerunning.

State/restore/checkpoint snapshot/prepare/notifications and split-return callbacks are
explicitly unsupported. Restart rereads all input; existing output blocks reruns. There
is no misleading no-op checkpoint or committer. A pipeline failure before terminal
flush should not publish, but previously published independent outputs are not rolled
back. SDK/parser diagnostics are redacted at adapter boundaries; packaged CLI suppresses
Hadoop/Parquet/ORC logs. Embedders must configure equivalent logging themselves.

## Dependencies and verification (not complete acceptance)

NIO is the path/publication owner. Commons CSV1.11.0/Jackson2.17.1 are already adopted.
New pins: parquet-avro1.15.0 (existing Parquet version), Avro1.12.0 (existing),
orc-core2.3.1, hive-storage-api2.8.1, hadoop-client-api/runtime3.4.3. Hadoop's official
shaded pair avoids packaging duplicate unshaded Hadoop classes and broad server trees;
Parquet's Hadoop dependencies are provided upstream, so the connector installs the
client pair itself. Library coordinates are Apache-2.0; complete transitive NOTICE/
license/security and convergence audits remain pending, not waived by this statement.
Shared codec policy aligns snappy1.1.10.8, at.yawk.lz4 1.11.2, zstd1.5.7-17 across
Kafka/ORC/Parquet; actual pinned compatibility is unverified.

**Local supplemental PASS:** 342 assertions with real CSV/JSON/text, source-built
Commons CSV1.11.0, IO2.16.1/codec1.17.0 and relocated WireMock Jackson; actual NIO
staging/hard-link publication, ServiceLoader and real LocalRunner. No production
source/sink/path/row stubs. That run excludes columnar adapters and uses an uncommitted
throwing columnar factory boundary, never a passing fake-format replacement.

An additional **452-assertion PASS** uses real CSV/JSON/text/**ORC**, unmodified ORC
code paths, and ParquetLocal compilation against jars extracted from PySpark4.1.0:
ORC2.2.1-shaded-protobuf, Hadoop3.4.2, Avro1.12.1, Parquet1.16.0, Jackson2.20.0,
hive-storage2.8.1 and other alternate transitive versions. The supplemental ColumnarFiles
copy disables only the unused Parquet branches. All ten example configs parse, and four real Fake→File→Console example pipelines pass
with those alternate jars. Kafka108/runtime310/TableMerge207 and their live bridge
regressions also pass in the supplemental environment (not pinned dependency proof).

**Parquet was not run**: source-building
parquet-avro was blocked by unavailable fastutil. This is not the pinned Maven graph,
not an all-production-source compile, and not all-five-format acceptance. No alternate
jars, patched libraries, test stubs or large artifacts are committed.

All-five-format JUnit contracts and installed-CLI E2E/log-suppression assertions are
added but **UNCOMPILED/UNRUN with pinned dependencies**. Actual production Parquet
branches remain uncompiled/unrun; pinned ORC2.3.1/Hadoop3.4.3, Maven/convergence,
formatter1.19.2, installed shaded CLI, full reactor and complete transitive security/
license gates remain UNRUN. Local formatter1.19.1/XML/whitespace checks only. No Actions
used. File/Kafka/streaming/Stage7 acceptance remains unchecked; prior gaps unchanged.

Required operator gates:

```sh
./mvnw -B -ntp -pl connector-file,seatunnel-cli -am verify
./mvnw -B -ntp verify
```

Tests use real temporary directories and delete them. There is no service/Docker gate
for local files. Test on the target OS/filesystem too, especially hardlink support.
