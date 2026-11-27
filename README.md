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
| 7a3 | Jayway JSON-path extraction | Merged by request; supplemental checks only |
| 7a4 | Library-backed SQL predicate filter | Merged by request; supplemental local checks only |
| 7a5 | SQL field selection/order/aliases | Merged by request; supplemental checks only |
| 7a6 | Bounded SQL string functions | Merged; reactor verified in PR #16 |
| 7a7 | Bounded SQL integer expressions | Merged by request; supplemental checks only |
| 7a8 | Signed integer literals for sql-long | Merged by request; supplemental checks only |
| 7a9 | Plain string and NULL literals for sql-string | Merged by request; supplemental checks only |
| 7a10 | Bounded SQL IN and BETWEEN filters | Draft; supplemental checks only |
| 7a11+ | Advanced transforms, broader expressions, connectors | ⬜ |

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
Advanced transforms and general computed SQL projection remain deferred. JSON-path is introduced in 7a3; SQL filtering in 7a4, field projection in 7a5 and bounded SQL string expressions in 7a6.

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

## SQL predicate filtering (Slice 7a4)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-filter-console.conf --mode local
```

`sql-filter` takes exactly `condition`, a predicate rather than SELECT text. Example:
`id >= 12 AND name <> 'row-13'` preserves rows 12 and 14 from the five-row example.
It preserves schema/row identity and drops SQL FALSE and UNKNOWN. JSqlParser 4.9
validates an explicit AST allowlist; a private HSQLDB 2.7.4 prepared VALUES-row query
owns SQL evaluation. Values are JDBC-bound, never interpolated. HSQLDB comparison/
collation semantics apply. No general SQL execution, projections or stored tables.

Supports AND/OR/NOT, parentheses, comparisons, IS NULL/IS NOT NULL, integer/string/
NULL/boolean literals and exact-case unqualified field names. BOOLEAN/INT/LONG/STRING
schemas only; field names must be simple ASCII identifiers (excluding TRUE/FALSE/NULL).
Functions, casts, arithmetic, subqueries, parameters and qualified names reject before
opening. SQL type compatibility is checked at open before source reading. Conditions
are trusted configuration, not a CPU sandbox; see [contracts](docs/SLICE-7A4.md).

Local source-built parser/engine checks are supplemental. Maven/JUnit, packaged CLI
E2E and full reactor verification remain unrun; no green-build claim or new Actions.

## SQL field projection (Slice 7a5)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-project-console.conf --mode local
```

`sql-project` accepts exactly `columns`, a SQL SELECT **list**, for example
`name AS label, id AS event_id` (not `SELECT ... FROM ...`). It selects, drops,
reorders and aliases existing columns. The example composes after `sql-filter`:

```text
["row-12",12]
["row-14",14]
```

JSqlParser 4.9 parses the entire list; only unqualified column references and explicit
`AS` aliases are allowed. Names are exact-case simple ASCII identifiers, output names
must be unique, and wildcard/functions/literals/computed expressions/clauses reject
before source reading. Repeat a source only with distinct output aliases. All source
types/nullability are preserved without SQL conversion, and each input emits one new
row; mutable field objects remain shared as in `SeaTunnelRow`. Conditions belong in a
preceding `sql-filter`, not in `columns`. See [bounds and contracts](docs/SLICE-7A5.md).

Local source-built parser checks and the real LocalRunner example passed. This is
supplemental verification, not a pinned Maven build or packaged CLI E2E result.

## SQL string expressions (Slice 7a6)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-string-console.conf --mode local
```

`sql-string` accepts exactly `expression` and `target`, appending one nullable STRING.
For example, `expression = "UPPER(name)", target = upper_name`. Supports STRING field
references plus nested `UPPER(x)`, `LOWER(x)`, `CONCAT(x,y)` and `COALESCE(x,y[, ...])`
(up to eight COALESCE arguments). HSQLDB owns casing and NULL semantics; this is not a
promise of vendor-independent SQL behavior. Arithmetic, casts, parameters,
qualified names and other/decorated functions are intentionally rejected. Use
`sql-project` afterward to select the computed field and rename/drop originals.

Shares the filter's private JDBC row-query resource lifecycle and strict typed binding.
Input follows the filter's schema constraints. Inputs and result strings are bounded
at 1,048,576 UTF-16 units, failing rather than truncating oversized results. Expressions
are trusted config, not a hostile SQL/CPU sandbox. See [contracts](docs/SLICE-7A6.md).
The example emits `["ROW-10",10]` and `["ROW-11",11]`.

Local adopted-parser/engine checks and three actual LocalRunner examples passed;
Maven/JUnit, packaged CLI E2E and full-reactor verification remain unrun. No Actions.

## SQL integer expressions (Slice 7a7)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-long-console.conf --mode local
```

`sql-long` accepts exactly `expression` and `target`. Example:
`expression = "id * id", target = squared` appends nullable LONG. Select it afterward
using `sql-project`; the example emits `[10,100]` and `[11,121]`.

Supports existing INT/LONG fields with `+`, `-`, `*`, unary signs and parentheses.
Every reference is promoted to SQL BIGINT first (so INT addition does not overflow
at 32 bits). HSQLDB owns NULL propagation and signed 64-bit overflow errors; no
silent wrapping. Division/modulo, decimal/floating math, functions, casts,
parameters, qualified names and SQL statements reject before opening. Inputs follow
shared SQL schema/binding bounds; see [contracts](docs/SLICE-7A7.md).

The existing SQL append/JDBC machinery is shared with `sql-string`; original values
and metadata are preserved and each input emits a new row. Configuration is trusted,
not a CPU/memory sandbox. New local operator/example checks passed; Maven/JUnit and
packaged CLI E2E for this slice remain unrun. No Actions started.

**Verification checkpoint:** PR #16 passed full Maven reactor verification at
`032f66f`, including previously pending JUnit and packaged CLI E2E through Slice7a6,
then merged. Its logging fix reserves stdout for JSON data and sends logs to stderr.
That result does not verify the new Slice7a7 code.

## SQL integer literals (Slice 7a8)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-long-literals-console.conf --mode local
```

`sql-long` now also accepts signed decimal integer literals. Example:
`expression = "id * 10 - 1", target = adjusted` produces `[10,99]` and `[11,109]`
after projection in the example. Constant-only expressions such as `2 + 3 * 4` work
for each input row. Leading zeroes are decimal, not octal.

Literals must fit signed 64-bit bounds; direct `-9223372036854775808` is supported.
Out-of-range literal leaves reject before open, while arithmetic overflow is delegated
to HSQLDB and fails at preparation/evaluation. `-(9223372036854775808)` rejects because
its unsigned literal leaf is out of range; use a directly signed minimum literal.
NULL, strings, decimal/scientific/hex/binary literals remain unsupported, as do casts,
functions and division/modulo. No new options or dependencies; existing schema/row/
lifecycle bounds apply. See [contracts](docs/SLICE-7A8.md).

Local adopted-parser/engine checks and five actual LocalRunner examples passed.
New Maven/JUnit and packaged CLI verification remain unrun; no Actions started.

## SQL plain string and NULL literals (Slice 7a9)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-string-literals-console.conf --mode local
```

`sql-string` now supports standard single-quoted strings and `NULL` in the existing
function surface, for example `COALESCE(name, 'unknown')` or `CONCAT('user-', name)`.
Constant-only expressions such as `UPPER('Ada')` and `NULL` work per input row too.
Use doubled apostrophes (`'O''Reilly'`); backslashes are ordinary characters, not
escapes. Unicode and empty strings are supported. JSqlParser owns literal parsing/
serialization, and HSQLDB owns NULL/function behavior. Prefixes/dialect syntax such
as `N'x'`, `E'x'`, binary literals and dollar quotes remain unsupported, as do numeric/
date literals, casts, arithmetic, parameters and other functions.

The example emits `[10,"USER-ROW-10"]` and `[11,"USER-ROW-11"]`. No new plugin/options/
dependencies; existing schema/lifecycle/size bounds apply. See [contracts](docs/SLICE-7A9.md).
Local parser/engine checks and six real LocalRunner examples passed. New Maven/JUnit,
packaged CLI and full reactor gates remain unrun. No Actions started.

## SQL IN and BETWEEN filters (Slice 7a10)

```sh
seatunnel-cli/target/seatunnel-cli/bin/seatunnel --config examples/fake-sql-predicates-console.conf --mode local
```

`sql-filter` now supports `id IN (12, 14)` / `NOT IN` and
`id BETWEEN 11 AND 14` / `NOT BETWEEN`, in addition to its existing predicates.
The example combines both and emits `[12,"row-12"]` and `[14,"row-14"]`.

IN requires a flat parenthesized list of 1..64 existing supported scalars (including
schema fields, duplicates and NULL). BETWEEN is inclusive; scalar bounds may be fields.
HSQLDB owns all membership/range/NULL/negation semantics: for example, a nonmatching
`NOT IN (1, NULL)` is UNKNOWN, so the filter drops the row. Reversed endpoints are not
reordered in Java. Subqueries, tuples, functions/arithmetic, GLOBAL/Oracle decorations
and SYMMETRIC syntax reject before opening. Existing schema/binding/AST/lifecycle
bounds apply, with list containers/items counted. [Contracts](docs/SLICE-7A10.md).

Local parser/JDBC checks and seven actual LocalRunner SQL examples passed. New JUnit,
packaged CLI and full-reactor verification remain unrun. No Actions started.
