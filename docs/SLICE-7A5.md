# Slice 7a5 — SQL field projection

## Specification and adoption decision (before implementation)

PR title: `[Slice 7a5] JSqlParser-backed field projection`.
Affected module: `seatunnel-transforms-v2`; provider discovery, CLI example/E2E and
project status docs. No API/core/engine coupling or new dependency cycles.

Reuse centrally pinned `com.github.jsqlparser:jsqlparser:4.9` under its Apache-2.0
licensing option. Parse its dedicated SelectItemsList grammar and require EOF;
never split SQL by commas or write a SQL lexer. Compile an allowlisted AST to
positional field mappings. This is structural row selection, not expression
execution: JDBC/HSQLDB is deliberately NOT used, avoiding coercion of source types.
Reuse the existing FieldTransforms positional Mapping lifecycle via an internal
factory; no duplicate row mapper. Existing SeaTunnelRow/CatalogTable handle ownership. Reuse current
JUnit5/AssertJ and CLI subprocess test infrastructure. No new libraries.

## Public contracts

- Provider `sql-project`, exactly one string option `columns`, e.g.
  `name AS label, id`. Supply a SELECT list, not a full SELECT statement.
- 1..4096 UTF-16 chars; 1..128 output fields. Parse the whole input with JSqlParser.
  Each item must be an unqualified Column with an optional explicit `AS alias`.
  Reject implicit aliases, wildcard, qualified names, functions, literals, casts,
  arithmetic, subqueries, WHERE/FROM/ORDER BY clauses, semicolons and other trailing
  SQL tokens. Comments/whitespace accepted by the library are not SQL tokens.
- Field references and aliases use simple ASCII names matching
  `[A-Za-z_][A-Za-z0-9_]*`, at most 128 chars. Quoted identifiers are out of scope.
  Resolution is exact-case. Output aliases must be unique (case-sensitive).
  Repeat a source column only under different output names. Resolve all fields
  against the incoming schema at construction, before opening/reading a source.
- Preserve table name, source type and nullability; order follows SELECT list.
  All CatalogTable types are supported by identity-preserving positional selection,
  not SQL conversion/evaluation. No runtime value type checks beyond row arity.
- Emit exactly one newly allocated row per input; source row/field array unchanged.
  Mutable field objects remain caller-owned, as in SeaTunnelRow/field-copy.
- Standard lifecycle: open once, apply only while open, close idempotent including
  before open; reject reopen after close, null rows and wrong input arity. No owned
  external resources. Single-thread confined, like existing local transforms.
- Only trusted configuration: parser bounds/AST allowlisting are not a hostile
  input CPU/memory sandbox. No bespoke parser/scheduler/evaluator.

## Scope and verification

No computed expressions, SQL casts, aggregation, joins, predicate embedding, SELECT
statements, schema inference from runtime data, distributed execution or checkpoint
changes. Existing `sql-filter` composes before projection in an ordered local chain.
Expression projection must be a separately specified engine-backed extension.

Add JUnit public contracts, discovery coverage, FakeSource -> sql-filter ->
sql-project -> ConsoleSink example, and packaged CLI E2E. Attempt local compilation
and executable smoke/integration checks against adopted parser. Record exact checks
and distinguish source-built supplemental evidence from Maven-pinned validation.
Required Maven gates: affected module test, CLI verify and full reactor verify.
No Actions jobs: project-wide 15-minute conservative reservation is fully allocated;
use `[skip ci]` and a draft PR, without automatic merge. Do not reset/reclaim budget.

## Three-line handoff

This slice: selecting, ordering, dropping and explicitly aliasing existing fields.
Pending gates: report local results and any unrun Maven/CLI checks honestly.
Next: review/merge decision; computed expressions need their own dependency-backed plan.


## Local verification results

- JDK 17 production compilation against source-built upstream JSqlParser 4.9 passed.
  Its parser was generated using source-built JavaCC 7.0.13. No vendored binaries.
- 62 executable parser/projection assertions passed: selection/aliases/schema/null,
  row ownership, unsupported AST/partial input, lifecycle and every schema type.
- Real LocalRunner + classpath discovery + FakeSource -> sql-filter -> sql-project ->
  ConsoleSink passed using the checked-in example: two exact expected JSON rows.
  Integration used tagged source-built Config 1.4.3 and HSQLDB 2.7.4, plus WireMock
  3.13.2's shaded Jackson. Only a temporary out-of-tree JsonFormat import relink was
  made for that shaded Jackson; production repository code was not relinked.
  The supplemental classpath registered the two SQL transform providers, not every
  unrelated provider. Full ServiceLoader expectations are covered by added JUnit.
- Changed Java formatted/parsed with google-java-format 1.19.1 on JDK17; POM XML and
  `git diff --check` passed. This is not the pinned Maven Spotless gate.
- Added JUnit contracts and packaged CLI E2E were NOT executed. Affected Maven test,
  packaged CLI verify and full reactor verify remain unrun: Maven Central is outside
  the sandbox's outbound host allowlist. No dependency-convergence/full-build claim.

No Actions launched. Merge permission for PR #13 applied to that PR only.
