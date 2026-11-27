# Slice 7a11 — bounded SQL LIKE predicates

## Plan / adoption decision (before implementation)

Title: `[Slice 7a11] Bounded SQL LIKE and ESCAPE filters`.
Production module: seatunnel-transforms-v2 (SqlFilterPlan, filter query wiring and
shared SqlRowQuery config-string bindings), tests/CLI/example/docs.
Reuse pinned JSqlParser4.9 (Apache-2.0 option) LikeExpression/StringValue AST, literal
serialization/decoding, and HSQLDB2.7.4 (permissive) evaluation through SqlRowQuery.
No regex engine, custom wildcard parser/matcher/escaper, new dependency/plugin/API/
core/engine import or cycles. Optional ESCAPE decoding uses library StringValue API.

## Contracts

- Existing sql-filter condition gains LIKE/NOT LIKE. Left operand is an existing
  supported STRING scalar or NULL (string field/plain literal/scalar parentheses).
  Pattern must be a plain single-quoted SQL literal<=1024 decoded UTF-16 units or NULL.
  Dynamic field-supplied patterns reject construction. Pattern literals may contain
  SQL % and _ wildcard syntax; matching/collation/case/NULL belong entirely to HSQLDB.
- Optional ESCAPE must be a plain single-quoted SQL literal decoding to exactly one
  non-surrogate UTF-16 character. Standard doubled quotes accepted; backslashes are
  ordinary characters, explicitly disable parser backslash escaping. No dynamic/null/
  empty/multiple-character escapes. Surrogate pairs/isolated surrogates rejected to
  avoid ambiguity with engine character units. No custom validation of LIKE pattern
  syntax; dangling escapes and similar errors remain SQL prepare/evaluation failures.
- Reject ILIKE/RLIKE/REGEXP, LIKE BINARY, prefixed/dialect literal forms, arithmetic/
  functions/casts/subqueries and non-STRING left operand. Pattern<=1024 is a config
  bound, not a CPU sandbox or claim against worst-case engine pattern execution.
- Keep existing full-input4096/AST512/depth64/shared schema128/row1Mi/lifecycle/typed
  binding bounds. Count source/pattern/escape nodes. Only SQL TRUE retains original
  row, FALSE/UNKNOWN drop. No Java rewriting/evaluation or filtering on regex rules.
  Row data stays JDBC-bound; literals use library AST serialization, not raw input.
  Existing IN/BETWEEN/comparison syntax/semantics unchanged.

## Scope and verification

JUnit wildcard/empty/Unicode/NULL/negation/escape/quote/pattern-length/dialect-rejection
contracts, former LIKE rejection assertion updated, example FakeSource -> sql-filter
-> ConsoleSink and packaged CLI E2E. Local parser/JDBC checks and existing operator/
LocalRunner regressions, format Java, parse XML, diff whitespace. Clearly distinguish
source-built supplemental checks from pinned Maven/JUnit/packaged CLI/full reactor
verification. PR20 /7a10 already explicitly merged, no new permission to merge future
work. No Actions: both one-time authorizations spent (reserved25m/observed14m45s),
[skip ci] draft, no new allowance. Broader dialect/regex/dynamic patterns excluded.

## Three-line handoff

This slice: bounded config-literal LIKE/NOT LIKE patterns and one-character ESCAPE.
Pending: local evidence and unrun Maven/JUnit/packaged CLI verification gates.
Next: review/merge decision; no further Actions or regex/dialect expansion implied.

## Dependency probe / adjusted query-adaptation plan

Local pinned-source HSQLDB2.7.4 probe found a constant-pattern optimization quirk on
nullable derived VALUES columns: NULL NOT LIKE '%' incorrectly evaluates TRUE.
A JDBC-bound pattern evaluates NULL correctly. Therefore compile validated patterns/
escapes as typed VARCHAR JDBC parameters, retaining pattern NULL as a nullable bound
string value; bind them after source row fields in query/visitor order. Extend the
shared row-query constructor with immutable config-string parameters; existing callers
use an empty list. No SQL CASE/Java matcher/null evaluator or dependency-version change.
Regression must cover repeated rows and multiple LIKE predicates to verify bind order.


## Local results

JDK17 production compilation with tagged source-built JSqlParser4.9 (source-built
JavaCC7.0.13 generated parser)/HSQLDB2.7.4 passed; no artifacts vendored. 54 new LIKE
parser/JDBC assertions passed, including NULL/all-wildcard negation, quotes/escapes,
Unicode/empty/backslash, pattern1024/1025 bound, dialect/dynamic rejection and ordered
pattern/escape rebinding across multiple predicates/rows and input failure. Typed NULL
source casts supply SQL type context, without custom NULL logic. Pattern parameters
avoid the observed inline-constant optimization quirk; matching stays engine-owned.
201 existing assertions passed:47 IN/BETWEEN,57 string/filter,30 string literal,37
integer,30 integer literal; private database registry empty after checks. All eight
actual LocalRunner/discovery/FakeSource/ConsoleSink SQL examples passed, exact rows.

Supplemental integration uses tagged Config1.4.3 source and WireMock3.13.2 shaded
Jackson through temporary out-of-tree JsonFormat import relink, SQL providers only.
Not Maven-pinned/full installed CLI/discovery/convergence evidence. Changed Java
formatted/parsed with google-java-format1.19.1, POM XML and diff whitespace passed.
New/changed JUnit and packaged CLI E2E/affected Maven/full reactor remain unrun:
Maven Central outside outbound allowlist. No new Actions/automatic merge permission.
