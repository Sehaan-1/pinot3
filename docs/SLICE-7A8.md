# Slice 7a8 — signed decimal literals for sql-long

## Plan / adoption decision (before implementation)

Title: `[Slice 7a8] Signed BIGINT literals for SQL arithmetic`.
Affected production code: SqlLongPlan in seatunnel-transforms-v2 only; tests/example/
CLI E2E and docs. No new plugin, API/core/engine changes, dependencies or cycles.
Reuse pinned JSqlParser4.9 (Apache-2.0 option) AST and HSQLDB2.7.4 (permissive) execution.
Adopt JDK BigInteger.longValueExact for decimal literal range validation, not arithmetic
execution. No homemade lexer/parser or SQL NULL/overflow evaluator.

## Contracts

- Extend sql-long expressions with decimal LongValue AST literals and directly signed
  decimal literals, in [-9223372036854775808, 9223372036854775807]. Existing unary signs,
  parentheses, arithmetic +,-,* and field resolution remain unchanged. Constant-only
  expressions allowed; still require a valid input schema/row and append one LONG.
- Validate numeric token spelling as ASCII digits; interpret leading zeroes as decimal.
  Canonicalize in-range literal to signed decimal and render CAST('<decimal>' AS BIGINT).
  Only canonical digits/sign enter SQL; user input is never passed through as SQL text.
  Row data remains JDBC-bound by existing SqlRowQuery. No additional JDBC parameters
  needed for trusted validated config constants.
- Combine only a SignedExpression directly wrapping LongValue for range validation so
  Long.MIN_VALUE is representable. An out-of-range unsigned leaf inside parentheses
  rejects even if an outer sign could mathematically bring it in range. Runtime unary
  negation of valid Long.MIN_VALUE is HSQLDB overflow, not Java wraparound.
- Out-of-range literals reject construction before opening. Arithmetic between valid
  literals/fields remains engine-evaluated; overflow fails at prepare or evaluation
  with SQL cause retained, never wraps/truncates. No Java constant folding.
- Reject NULL/string/decimal/scientific/hex/binary literals, parameters, SQL statements,
  functions, division/modulo and casts. Full-input parser check, node64/depth16/expression
  4096 limits remain; sign + literal both count as AST nodes. Trusted config, no hard
  CPU/memory sandbox. Existing input binding/schema/target/row/lifecycle bounds remain.

## Scope / verification

No literal support in sql-string/filter/project changes, no decimals/division, no generic
computed SELECT, no wider schema support or connectors. Add JUnit literal boundaries,
leading-zero/sign/NULL/constant-only/overflow/recovery/rejection cases and executable
example FakeSource -> sql-long -> sql-project -> ConsoleSink with packaged CLI E2E.
Compile against source-built adopted parser/engine and run real LocalRunner examples
and supplemental regressions when available. Parse/format Java, XML, diff whitespace.
Record unrun Maven/JUnit/packaged CLI/full reactor honestly; previous PR16 green result
is not evidence for new slices7a7/7a8. No Actions: both one-time authorizations spent,
25m conservative reservations fully allocated, historical14m45s observed is not new
budget. Use [skip ci] draft; no automatic merge. PR17 merged explicitly by request.

## Three-line handoff

This slice: signed BIGINT decimal literals in existing sql-long expressions.
Pending: report local results and new Maven/JUnit/packaged CLI verification gaps.
Next: review/merge decision; no broader SQL scope or new CI authorization implied.


## Local results

JDK17 production compilation against tagged source-built JSqlParser4.9 (source-built
JavaCC7.0.13 grammar generation) and HSQLDB2.7.4 passed; no binaries vendored.
30 literal parser/JDBC checks, 37 updated integer regressions (two formerly rejected
literal cases are now supported), and 59 existing string/filter regressions passed.
Includes signed boundaries, constant arithmetic/overflow, NULL, range/spelling rejection,
row overflow/recovery and empty private database registry after checks. All five actual
LocalRunner/discovery/FakeSource/ConsoleSink SQL examples passed with exact expected rows.

Supplemental integration used tagged source-built Config1.4.3 and WireMock3.13.2 shaded
Jackson through only an out-of-tree temporary JsonFormat import relink. SQL providers
only on supplemental classpath; not pinned Maven/dependency-convergence/installed CLI
proof. Changed Java formatted/parsed with google-java-format1.19.1, POM XML/diff whitespace
passed. Added JUnit and packaged CLI E2E, affected Maven and full reactor remain unrun:
Maven Central is outside outbound allowlist. Prior PR16 green result covers its own
head only. No new Actions or automatic merge authorization.
