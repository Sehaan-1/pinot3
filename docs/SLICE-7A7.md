# Slice 7a7 — bounded SQL integer expressions

## Plan and adopt/build decision (before implementation)

Title: `[Slice 7a7] Library-evaluated SQL integer expressions`.
Module seatunnel-transforms-v2; CLI example/E2E and docs. No API/core/engine imports,
new dependencies or cycles. Reuse pinned JSqlParser4.9 (Apache-2.0 option) and
HSQLDB2.7.4 (permissive). Parser owns syntax; engine owns arithmetic/NULL/overflow.
Reuse SqlRowQuery's private JDBC lifecycle/bindings. Extract a package-private append
adapter shared by existing sql-string and new sql-long rather than duplicate row/schema
ownership. No homegrown SQL parser/evaluator, connector, scheduler or protocol.

## Contracts

- Plugin `sql-long`, exactly string options `expression`, `target`. Append one nullable
  LONG, preserve table name/input metadata and original values. Exactly one new row per
  input; mutable field references stay caller-owned, consistent with SeaTunnelRow.
- 1..4096 UTF-16 expression units, complete JSqlParser parsing. Supported AST: existing
  unqualified INT/LONG fields, binary + / - / *, unary + / -, parentheses. No constants
  (including NULL), division, modulo, decimal/floating math, casts, functions, parameters,
  SELECT/subqueries, qualified names or other tokens. Depth<=16, nodes<=64, trusted
  configuration only (not hostile parsing/CPU sandbox).
- Promote each INT/LONG field reference to SQL BIGINT before arithmetic; preserve
  parentheses/precedence in canonical SQL. HSQLDB evaluates signed 64-bit math and
  propagates NULL. Overflow fails with retained SQL cause, never wraps/truncates.
  No Java arithmetic evaluator. No cross-vendor SQL equivalence claim.
- Input schema follows shared SQL bounds: 1..128 BOOLEAN/INT/LONG/STRING, simple ASCII
  names<=128 excluding TRUE/FALSE/NULL. Only numeric fields can appear in expressions.
  Entire row uses exact Boolean/Integer/Long/String runtime types or null, and string
  inputs<=1,048,576 UTF-16 units, even when not referenced. New target uses simple ASCII
  name<=128, no existing exact-case name. Output can have129 columns; subsequent SQL
  transforms retain their own128-column input limit.
- Syntax/schema bind at construction, SQL prepare at open before reading. Shared
  single-thread-confined lifecycle: open once/apply only while open/idempotent close
  even before open, no reopen after close. Clear parameters even after failed binding,
  arithmetic or result decoding; close resources on startup/shutdown failure with
  secondary errors suppressed. One private in-memory database, no arbitrary JDBC mode,
  file/server/history; query timeout/one-row cap only defense in depth.
- Existing sql-string public behavior, result bound and generic errors remain unchanged
  by append-adapter extraction. Filter and field-only projection untouched.

## Verification and scope

JUnit schema/value/NULL/overflow-recovery/lifecycle/options/AST/bounds contracts, discovery
expectation, FakeSource -> sql-long -> sql-project -> ConsoleSink example and packaged
CLI E2E. Local adopted parser/engine compilation/smokes and real LocalRunner examples;
format Java, parse XML, diff whitespace. Supplemental source-built dependencies are
not pinned Maven evidence. Required affected Maven tests/CLI verify/full reactor gates
remain pending unless actually run. Full reactor on032f66f passed in PR16 (now merged),
not proof of this new code. No Actions allowed: earlier one-time grants are spent.
Use [skip ci] draft, no automatic merge. Ledger historical observed14m45s and reserved
25m are not new allowance. Broader computed projection, division/decimals/literals,
aggregation/joins, distributed execution and connectors remain out of scope.

## Three-line handoff

This slice: nullable signed LONG arithmetic via adopted parser/embedded engine.
Pending: record local results; Maven/JUnit/packaged CLI gates require verification.
Next: review/merge decision; no further CI or expanded expression surface authorized.


## Local results

- JDK17 production compilation with tagged source-built JSqlParser4.9/HSQLDB2.7.4:
  passed. Parser generated with source-built JavaCC7.0.13. No vendored artifacts.
- 39 executable integer parser/JDBC assertions: passed, including INT promotion,
  precedence/signs/NULL, overflow errors/recovery, AST rejection and lifecycle.
- 59 existing string/filter parser/JDBC regression assertions: passed after shared
  append extraction. Private HSQLDB registry empty after checks.
- Actual LocalRunner + discovery + FakeSource/ConsoleSink examples sql-long,
  sql-string, sql-project and sql-filter: passed with exact expected output rows.
- Supplemental integration used source-built Config1.4.3 and WireMock3.13.2 shaded
  Jackson via only an out-of-tree temporary JsonFormat import relink. Only SQL
  providers registered in supplemental classpath. Not a Maven-pinned artifact graph,
  full provider-discovery check, installed CLI/logging or dependency-convergence proof.
- Changed Java formatting/parsing with google-java-format1.19.1, POM XML and
  git diff --check: passed. Not the pinned Maven Spotless gate.
- Added JUnit/discovery/packaged CLI E2E remain unrun; affected Maven/full reactor
  checks unavailable locally (Maven Central outside outbound allowlist). No new CI.
