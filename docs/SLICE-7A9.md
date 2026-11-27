# Slice 7a9 — plain string and NULL literals for sql-string

## Plan / adopt-vs-build decision (before implementation)

Title: `[Slice 7a9] Plain string literals and SQL NULL for string expressions`.
Only production change: SqlStringPlan, seatunnel-transforms-v2. Reuse centrally pinned
JSqlParser4.9 (Apache-2.0 option) StringValue/NullValue AST and literal serialization,
and HSQLDB2.7.4 (permissive) execution. No custom quote decoder/escaper, SQL parser,
NULL/function evaluator, new dependency, plugin, API/core/engine import or cycle.

## Contracts

- Existing sql-string expression/target options gain plain single-quoted SQL string
  literals and NULL at any accepted argument/root position. Existing field references,
  UPPER/LOWER/CONCAT/COALESCE allowlist/arity/decoration rejection remains unchanged.
  Examples: COALESCE(name,'unknown'), CONCAT('user-',name), UPPER('Ada'), NULL.
- Plain literals use standard doubled apostrophes, e.g. 'O''Reilly'. Backslashes are
  ordinary characters, not escape sequences; configure library parser explicitly.
  Unicode/empty strings accepted. Reject all prefixed/dialect literal forms (N/E/U/R/
  B/RB/_utf8/Q), numeric/boolean/date/binary literals, dollar quotes, parameters,
  statements, casts, arithmetic and other functions. Library StringValue.getPrefix
  defines prefix rejection; library serializer re-emits the validated plain literal.
- Cast accepted literal/NULL nodes to SQL VARCHAR(1048576) for explicit result typing,
  including root NULL and all-NULL COALESCE. NULL/function behavior is HSQLDB-owned;
  do not Java-fold constants or simulate truth/NULL semantics. Literal-only expression
  still requires a valid input schema and exact-arity/type row through shared JDBC.
- Retain 4096 UTF-16 expression units,64 AST nodes/depth16,128 input columns, shared
  schema/binding/target/lifecycle rules and1Mi string input/output bounds. Each literal
  counts as one node. Configuration literals use library-rendered SQL; row data stays
  JDBC-bound. No unrestricted SQL interpolation. Trusted config, not a hostile SQL/
  CPU/memory sandbox. Existing error/cause/cleanup/one-new-row contracts unchanged.

## Scope and verification

No numeric literal changes, division/decimals, general SELECT expressions or expanded
function registry. Add JUnit quote/backslash/unicode/empty/NULL/fallback/root/constant
and prefix/rejection tests, new real FakeSource -> sql-string -> sql-project -> console
example and packaged CLI E2E. Run local production compilation/adopted-engine checks,
existing regressions and actual LocalRunner examples. Format Java/XML/whitespace checks.
Report Maven/JUnit/packaged CLI/full reactor unrun unless actually executed. Prior
PR16 success at032f66f verifies through7a6 only; PR18/7a8 merged on explicit request.
No Actions: spent one-time grants, reserved25m/observed14m45s unchanged, no new allowance.
Use [skip ci] draft, no automatic merge. Broader SQL scope remains separately specified.

## Three-line handoff

This slice: standard plain string/NULL literals in existing sql-string functions.
Pending: local results plus explicit Maven/JUnit/packaged CLI verification gaps.
Next: review/merge decision; no additional Actions or SQL dialect expansion implied.


## Local results and gaps

JDK17 production compilation with tagged source-built JSqlParser4.9 (JavaCC7.0.13
source-built generation) / HSQLDB2.7.4 passed. No binaries vendored. 30 new string/NULL
parser/JDBC assertions passed, including quotes, ordinary backslashes, Unicode, empty
strings, constant/fallback/all-NULL expressions, injection-shaped text and prefixed/
malformed rejection. Lexical TokenMgrException is now wrapped as IllegalArgumentException
at construction, retaining cause, just like other parser failures.
57 updated string/filter regression assertions (two old literal rejections now supported),
37 integer and30 integer-literal regressions passed; private database registry empty.
All six actual LocalRunner/discovery/FakeSource/ConsoleSink SQL examples passed, exact
rows. Supplemental integration uses source-built Config1.4.3 and WireMock3.13.2 shaded
Jackson via only an out-of-tree temporary JsonFormat import relink, SQL providers only.
Not a pinned Maven dependency graph/full installed CLI/discovery/convergence claim.
Changed Java formatted/parsed with google-java-format1.19.1, POM XML/diff whitespace
passed. Added/changed JUnit and packaged CLI E2E, affected Maven/full reactor remain
unrun (Maven Central outside outbound allowlist). No Actions or automatic merge.
