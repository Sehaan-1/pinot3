# Slice 7a4 — SQL predicate filtering with adopted parser and engine

## Specification and dependency plan

PR title: `[Slice 7a4] Parsed SQL filters with embedded HSQLDB evaluation`.
Module `seatunnel-transforms-v2`; no API/core/engine changes or cycles.

Adopt `com.github.jsqlparser:jsqlparser:4.9` under its Apache-2.0 licensing option
for complete predicate parsing and AST validation. Adopt `org.hsqldb:hsqldb:2.7.4`
(BSD-style HSQLDB license) as a private embedded JDBC evaluator. Pin both centrally.
These libraries have separate jobs: parser defines an explicit supported AST surface,
SQL engine owns comparison and three-valued null semantics. Do NOT implement a custom
SQL evaluator. HSQLDB is chosen over a homemade evaluator and over a broad Calcite
runtime for this small fixed-schema, one-row filter. JDK JDBC owns resource contracts;
existing discovery/HOCON and JUnit/AssertJ/Failsafe remain. No new scheduler/protocol.

## Contracts

- Installed plugin `sql-filter`, exactly one string option `condition` (1..4096 chars).
  This is a predicate, not SELECT text. JSqlParser must consume the entire input.
  Supported AST: AND/OR/NOT, parentheses, =/<>/!=/</<=/>/>= comparisons, IS NULL /
  IS NOT NULL, integer/string/NULL/TRUE/FALSE literals and unqualified field names.
  Reject functions, arithmetic, casts, JDBC parameters, subqueries, joins, LIKE,
  IN, BETWEEN, qualified names and all statements. No projection in this slice.
- Schema has 1..128 columns, each BOOLEAN, INT, LONG or STRING. Exact-case names must
  match `[A-Za-z_][A-Za-z0-9_]*`, be at most 128 chars, and not be TRUE/FALSE/NULL
  (reserved for literals). SQL identifiers are quoted by the AST adapter. Other
  schema types reject construction rather than silently coerce/truncate values.
- Factory parses and validates schema/AST without opening resources. Semantic SQL
  type checking/preparation occurs in open, before source reader startup. AST walk
  has node/depth limits (512/64). Conditions are trusted configuration: library
  parsing/timeouts/allowlists are NOT an adversarial SQL or CPU sandbox.
- One private named in-memory HSQLDB connection and prepared single-row query per
  operator. No tables or stored row history: a derived VALUES row receives JDBC-bound
  inputs. Database URI/credentials are engine-selected, not supplied by users. No
  server/filesystem mode, arbitrary statements or user-controlled JDBC settings.
- Preserve schema and original row. Emit that row only when SQL WHERE is TRUE;
  FALSE and UNKNOWN emit nothing. Delegate semantics/collation to HSQLDB, not a
  promise of every vendor's SQL dialect. Only exact runtime Boolean/Integer/Long/
  String cell types (or null) bind; strings have a 1,048,576 UTF-16 unit bound.
  Nulls are permitted even on nonnullable source metadata, matching prior runtime.
- Bind data through PreparedStatement, never interpolate row contents into SQL.
  Clear parameters after each evaluation. One-row result limit and driver query
  timeout are defense-in-depth, not hard wall-clock cancellation guarantees.
- Standard single-thread-confined lifecycle: open once, apply only when open,
  idempotent close even before open. Close statement/connection on partial startup
  and shutdown, preserving suppressed cleanup failures. Last connection closes the
  private in-memory database via `shutdown=true`. Errors expose generic top-level
  preparation/evaluation messages, retaining original causes for debugging.

## Verification and budget

Zero Actions jobs; `[skip ci]` head and draft PR. No resetting/reclaiming the existing
project-wide reservation without user authorization. Required unrun Maven gates:
`./mvnw -pl seatunnel-transforms-v2 -am test`, CLI verify and full reactor verify.

For local verification, build JSqlParser 4.9 from upstream tagged source and generate
its parser using source-built JavaCC 7.0.13; build HSQLDB 2.7.4 from its upstream tag.
Use JDK17 to compile the production implementation and run real JDBC predicate/null/
resource/lifecycle smoke checks. No source/binaries vendored in Git. Add JUnit contracts
and packaged CLI subprocess E2E for eventual Maven verification. Local source builds
are supplemental, not a Maven dependency-convergence/full-build claim. Format/parse
Java, parse POM XML, and check whitespace locally.

## Out of scope and handoff

No SELECT/projection, arithmetic/functions, unsupported schema types, SQL mutation,
stream joins/aggregation, distributed ETL, checkpoint wiring or SQL server launcher.

Next: SQL projection as a separately bounded, library-backed slice.
This is a row-preserving filter, not a general SQL execution service.
Review/merge and reconcile the project-wide CI budget before automated verification.

## Local results

JDK17 production compilation against source-built tagged JSqlParser: passed.
32 real parser/JDBC operator smoke assertions: passed (truth, NULL, binding,
injection-shaped row value, allowlist rejection, lifecycle and recovery).
20 open/evaluate/close cycles plus a SQL-error path: private HSQLDB registry returned
to its initial size. POM XML, Java formatting/parsing and `git diff --check`: passed.
JUnit tests and packaged subprocess E2E have been added but not executed. Real
FakeSource→LocalRunner→ConsoleSink integration and full Maven dependency graph/build
are also unrun in this environment. These remain review/verification gaps, not passes.
