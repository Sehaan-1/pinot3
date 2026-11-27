# Slice 7a6 — bounded SQL string expressions

## Plan and adoption decision (before production changes)

Title: `[Slice 7a6] Library-evaluated SQL string expressions`.
Module: seatunnel-transforms-v2, plus CLI E2E/example and documentation; no API/core/
engine changes, no cycles. Reuse pinned JSqlParser 4.9 (Apache-2.0 option) and HSQLDB
2.7.4 (permissive) from 7a4. No new dependencies or homemade SQL evaluator/parser.
Extract the existing filter's JDBC lifecycle and typed row binding into a package-
private SqlRowQuery shared by both transforms. Preserve filter public contracts.

## Contracts

- Provider `sql-string`, exactly `expression` and `target` string options. Append one
  nullable STRING column; preserve table name, existing fields/type/nullability and
  input row. Emit exactly one new row per input, retaining mutable cell references.
- Expression is 1..4096 UTF-16 units, parsed completely by JSqlParser. AST allowlist:
  unqualified existing STRING columns and nested UPPER(x), LOWER(x), CONCAT(x,y),
  COALESCE(x,y[, ...]) with 2..8 arguments. At most 64 nodes and depth 16. Reject
  literals, parameters, arithmetic, casts, other functions, DISTINCT/ALL/UNIQUE,
  named arguments, function attributes, analytic calls, subqueries and statements.
- Build canonical SQL only from validated fields and whitelisted function names;
  reject function decorations rather than silently discard them. All row values
  are JDBC-bound in a derived VALUES row. HSQLDB, not Java, owns function, Unicode,
  comparison and NULL behavior. No portable vendor-independent SQL semantics claim.
- Target is a new exact-case simple ASCII identifier of at most 128 chars. Input
  schema follows filter's constraints: 1..128 BOOLEAN/INT/LONG/STRING columns with
  simple names, excluding TRUE/FALSE/NULL. Expressions can reference STRING only.
  Output may have 129 columns (as with other append transforms); a subsequent SQL
  transform still enforces its own 128-column input bound.
- Each input string is at most 1,048,576 UTF-16 units; require exact runtime types
  Boolean/Integer/Long/String or null for all input cells, as in sql-filter. Result
  string bound is also 1,048,576 units; oversize fails, never silently truncates.
- Resolve syntax/schema at construction. Prepare/check SQL types at open before
  source reading. New private in-memory connection per operator, no external JDBC
  settings, servers, stored history or filesystem mode. One-row result/query timeout
  as defense in depth, not a hard wall-clock sandbox. Trusted config only.
- Reuse open-once/apply-while-open/idempotent-close lifecycle, clear parameters after
  each evaluation including failures, close resources on partial startup/shutdown,
  suppress secondary cleanup failures. Single-thread confined. SQL error top-level
  messages stay generic with original causes retained. No output row on failure.

## Bounded scope

No general computed SELECT projection, typed numeric expressions, literals, generic
function registry, arithmetic, joins/aggregation, distributed scheduling/checkpoints,
connector work or expanded SQL dialect. Existing field-only sql-project is unchanged.

## Verification plan and budget

Add JUnit expression/schema/null/ownership/lifecycle/bounds contracts and filter
regression checks for the shared JDBC extraction. Add FakeSource -> sql-string ->
sql-project -> ConsoleSink example and packaged CLI E2E. Locally compile production
and run real adopted parser/engine checks plus actual LocalRunner example when
possible. Format/parse changed Java, parse POMs, git diff --check. Report supplemental
source builds separately from pinned Maven/JUnit/full integration gates.
Required remaining gates: affected Maven tests, CLI verify, full reactor verify.
Zero Actions: all 15 project-wide conservative reservation minutes remain allocated.
Use [skip ci], draft PR, no automatic merge. PR14's explicit merge override is scoped
to PR14 and does not grant new CI or merge authorization.

## Three-line handoff

This slice: bounded engine-evaluated string expressions, not general SQL projection.
Pending: record actual local evidence and unrun Maven/CLI verification gates.
Next: review/merge decision; broader computed expressions require another bounded plan.


## Local results and known verification gaps

JDK17 production compilation against tagged source-built JSqlParser 4.9 and HSQLDB
2.7.4: passed. Source-built JavaCC 7.0.13 generated the parser. No binaries vendored.
59 real parser/JDBC smoke checks passed, including nested functions/NULL/bound data,
unsupported AST/decorations/arity, lifecycle, oversized result rejection and filter
truth/NULL regressions after shared JDBC extraction. Private database registry ended
empty. All three actual LocalRunner/classpath discovery + FakeSource/ConsoleSink
examples (sql-string, sql-project, sql-filter) passed with exact expected two rows.

Supplemental integration used tagged source-built Config 1.4.3 and WireMock 3.13.2's
shaded Jackson, with only an out-of-tree temporary JsonFormat import relink. Only the
three SQL providers were registered on the supplemental classpath. Not a pinned
Maven dependency graph/full installed-provider verification claim. Changed Java
formatted/parsed with google-java-format 1.19.1; POM XML/diff whitespace passed.

Added JUnit and packaged CLI E2E are unrun, as are affected Maven/full reactor gates;
Maven Central is outside the outbound host allowlist. No green-build claim. Shared
JDBC SQL-error wrappers now use generic `SQL row query ...` messages rather than
filter-specific wording; cause retention, lifecycle, schema and row behavior remain.
No new Actions or automatic merge authorization.
