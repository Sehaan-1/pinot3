# Slice7f — bounded local files, one cohesive source/sink PR

## Adoption / scope fixed before implementation

Use JDK17 NIO local filesystem; existing Jackson2.17.1 and Commons CSV1.11.0;
Apache Parquet Avro/Hadoop1.15.0 (align existing parquet-format-structures), Avro1.12.0,
Apache ORC core2.3.1, Hadoop client-api/client-runtime3.4.3.
ORC uses hive-storage-api2.8.1 transitively; pin it explicitly. All Apache-2.0. ORC tag and Hadoop release tag checked on GitHub; exact dependency
convergence/security/build remain verification gates, not inferred from source review.
Reuse library parsers, encoders and vector batches; never implement file format codecs.
JDK NIO rather than full hadoop-client distribution for local path ownership.

One `connector-file` module, `file` providers, CLI dependency, examples, tests/docs.
All five formats implemented together: CSV RFC4180 no header, JSON one schema-ordered
array per UTF8 line, text one non-null STRING per UTF8 line, uncompressed Parquet
Avro primitive records, uncompressed ORC primitive structs. Schema primitive types:
BOOLEAN/INT/LONG/FLOAT/DOUBLE/STRING/BYTES; decimal/temporal/nested types rejected until
precision/timezone/nested contracts exist. Binary textual representation base64.
CSV null marker literal `\\N`, reserved (literal same string rejected), distinct from
empty. JSON native null; text no null or embedded CR/LF. CSV handles quoted multiline.
Nonfinite floats rejected. Strict schema/arity/nullability/kinds/types.

## Options / local ownership

`root` absolute existing directory, source `paths` explicit unique relative file list
(1..128); sink `path` single relative output file. No URI/glob/listing/append/overwrite.
Reject absolute children, dot/dotdot segments, symlinks on any component, missing
parents, nonregular files, source file mutation (size/mtime/fileKey best effort).
Root and parents must be trusted against concurrent hostile mutation; this is not a
SecureDirectoryStream sandbox. No resources or filesystem IO in plugin factories.
`format`: csv/json/text/parquet/orc. Source `schema` list of {name,type,nullable}; sink
uses input table, no redundant schema. Column names ASCII identifiers, max128.
`max-file-bytes` default64MiB (1..1GiB), `max-row-bytes` default1MiB (1..16MiB).
Limits bound ordinary payloads, not hostile columnar decompression/parser allocation;
trusted local inputs only. No hard IO deadline/cancellation guarantee on local devices.

## Source / sink semantics

Bounded single reader/enumerator only. One split per explicit file, deterministic list
order. Enumerator runtime start fingerprints files; reader runtime opens one at a time,
emits one row/poll. Validate split identity, no duplicate/foreign assignment. Close on
partial open, parse, output failure, interrupt; mutation checks before/after read.
No checkpoint/restore for this slice: reject restored states and snapshot/prepare/notify
operations explicitly; no false replay/exactly-once claims. Pipeline restart rereads.

Sink INSERT-only. Runtime creates same-directory unique staged file; no final file
until flush(true) closes library footer, checks limits, forces staged data then publishes
using atomic no-clobber NIO hard-link creation and unlinks staging. Unsupported hardlinks
fail rather than non-atomic fallback. Existing target never overwritten. No directory
creation, no empty pre-created placeholder, no transaction/committer. close/failure
before terminal flush discards stage; after publication leaves target. No directory fsync,
crash recovery or cross-filesystem/exactly-once guarantee. Failed run can leave stage
files on process death; operator cleanup only when no writer owns them. flush(false)
never publishes. State unsupported; LocalRunner bounded integration only.

## Gates / authorization

Real temp-directory roundtrips for every format (including null/bytes), empty files,
multiline CSV, malformed/schema mismatch, ownership/cleanup/limits/interruption/path
safety, all row kinds, no-clobber publication, LocalRunner and packaged CLI. Spec first,
then implementation, supplemental local verification, one PR then merge per user.
No new Actions. Actual pinned Maven/full reactor, Parquet/ORC and packaged acceptance
must remain UNRUN if artifacts unavailable; deterministic substitutes are not format
proof. Prior JDBC/Kafka/streaming/7b gaps stay carried forward; Stage7 unchecked.

### Dependency review refinement (before final integration)

ORC2.3.1 itself uses Hadoop's official shaded client-api/client-runtime3.4.3 pair.
Adopt that pair instead of also packaging unshaded common/mapreduce classes; Parquet
marks Hadoop dependencies provided. This avoids duplicate Hadoop classes and unshaded
server dependencies. Keep APIs available at runtime (not provided). Align shared native
codecs with Kafka/ORC: snappy1.1.10.8, at.yawk.lz4 1.11.2, zstd1.5.7-17. This is a
version policy, NOT a tested convergence/codec compatibility/transitive license audit.

## Result / gates

Implemented all five library-backed file formats, single-reader source, staged no-clobber
sink, strict schema/path/lifecycle, CLI providers/dependency/logging, ten runnable example
configs, real temp-directory contract tests and installed CLI all-format roundtrips.
Supplemental PASS342 assertions CSV/JSON/text + real LocalRunner; additional PASS452
CSV/JSON/text/ORC using alternate PySpark-distributed jars. Production Parquet branches
remain uncompiled/unrun (source-build dependency fastutil unavailable); ORC path unchanged
but only tested with2.2.1, not pinned2.3.1. Exact evidence in [FILES.md](FILES.md).
Actual pinned columnar/JUnit/CLI/Maven/convergence/full reactor/security/license/format
checks UNRUN. No Actions. Merge is explicitly requested for this one slice, not permission
for subsequent merges. File/Stage7 acceptance deliberately unchecked.
