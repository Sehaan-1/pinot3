# Slice 7a10 — bounded IN and BETWEEN predicates

## Plan / adoption before implementation

Title: `[Slice 7a10] Bounded IN and BETWEEN SQL filters`.
Only production module: seatunnel-transforms-v2, SqlFilterPlan. Reuse pinned
JSqlParser4.9 (Apache-2.0 option) AST/serialization and HSQLDB2.7.4 (permissive)
query evaluation, plus shared SqlRowQuery. No new dependencies, parser/evaluator,
plugin/options, API/core/engine imports or cycles.

## Contracts

- Extend existing sql-filter condition with scalar IN / NOT IN (1..64 scalar items)
  and scalar BETWEEN / NOT BETWEEN start AND end. Inclusive/negation/NULL behavior
  belongs entirely to HSQLDB, not Java rewriting into comparisons/list membership.
- IN requires a parenthesized flat expression list, not subqueries, tuple lists,
  table expressions or bare values. Each item and left side uses existing supported
  scalar grammar: schema fields, signed integer/string/boolean/NULL literals and
  scalar parentheses. Duplicates allowed; field references inside lists are allowed.
- BETWEEN accepts existing scalars on each side/end; endpoints can be schema fields.
  No arithmetic/functions/casts/subqueries. No SYMMETRIC/ASYMMETRIC or vendor extensions;
  GLOBAL IN and Oracle (+)/PRIOR join syntax reject before open.
- Full-input JSqlParser consumption; condition<=4096 UTF-16 units, AST<=512 nodes,
  depth<=64, including list container/items and signed literal children. Reject empty/
  oversized lists at construction. Scalar identifier binding stays exact-case and
  schema-bound. Reject Column array/comment modifiers instead of forwarding unvalidated
  attached expressions; applies consistently to existing comparisons and new predicates.
- Existing field/type/nullability/row identity and lifecycle contracts unchanged.
  Only SQL TRUE retains original row; FALSE/UNKNOWN drop. No homemade truth or NULL
  comparisons, deduplication, sorting or endpoint normalization. Incompatible SQL types
  may reject during open; query evaluation errors retain SQL causes. Row values JDBC-
  bound; generic trusted config, not hostile SQL/CPU sandbox. Normalize lexer failures
  as IllegalArgumentException at construction, retaining cause.

## Scope / verification

No subqueries/joins, arbitrary functions, LIKE, quantified comparisons, SQL mutation,
projection changes, numeric literal dialect expansion or connector/distributed work.
JUnit true/false/NULL/NOT/OR/endpoint/duplicates/list limits/subquery/modifier/lexical
contracts; update previous IN/BETWEEN rejection expectations. Example FakeSource ->
sql-filter -> ConsoleSink and packaged CLI E2E. Local adopted-parser/JDBC checks and
actual LocalRunner examples; format Java/XML/whitespace checks. Record unrun affected
Maven/JUnit/CLI/full reactor. Prior PR16 green head032f66f doesn't cover new slices.
No Actions: one-time grants spent, reserved25m/observed14m45s unchanged, [skip ci] draft,
no automatic merge. PR19/7a9 explicitly merged by request; no blanket future waiver.

## Three-line handoff

This slice: bounded flat IN lists and scalar BETWEEN with engine-owned SQL semantics.
Pending: actual local results and new Maven/JUnit/packaged CLI verification gaps.
Next: review/merge decision; no expanded SQL dialect or additional Actions implied.


## Local results

JDK17 production compilation against tagged source-built JSqlParser4.9 (source-built
JavaCC7.0.13 generated parser) and HSQLDB2.7.4 passed. No binaries vendored. 47 new
IN/BETWEEN parser/JDBC assertions passed: truth/negation/NULL/inclusivity/reversed
bounds, duplicates/fields/single item,64-vs65 limit, subquery/tuple/function/decorated/
malformed rejection and original-row identity. Existing regressions passed:57 string/
filter,30 string literals,37 integer and30 integer literals (154 total). Registry empty.
All seven actual LocalRunner/discovery/FakeSource/ConsoleSink SQL examples passed with
exact expected output rows. Added/changed JUnit and packaged CLI E2E remain unrun.

Supplemental integration uses tagged Config1.4.3 source and WireMock3.13.2 shaded
Jackson via only temporary out-of-tree JsonFormat import relink, SQL providers only.
Not a pinned Maven/full installed CLI/discovery/convergence claim. Java formatting/
parsing (google-java-format1.19.1), POM XML/diff whitespace passed. Affected Maven/full
reactor not run: Maven Central outside outbound allowlist. No new Actions; prior PR16
verification doesn't cover this code. No automatic merge authorization.
