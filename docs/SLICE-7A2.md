# Slice 7a2 — Literal string replace/split transforms

## Specification and dependency plan

PR title: `[Slice 7a2] Literal string replace and bounded split transforms`.
Module: `seatunnel-transforms-v2`, packaged automatically by the existing CLI dependency.
No API/core/engine changes, new module, dependency or circular dependency.

Adopt JDK17 `String.replace(CharSequence, CharSequence)` for literal replacement,
`String.split(Pattern.quote(separator), positiveLimit)` for bounded literal splitting,
and existing Typesafe Config 1.4.3/ServiceLoader for configuration/discovery. Do not
invent a delimiter parser or expose regex evaluation. Reuse existing row/schema SPI,
JUnit 5.11.0, AssertJ 3.26.3 and Failsafe 3.2.5. No new coordinates to pin.

## Contracts

- `string-replace`: exactly `field`, `search`, `replacement` string options. Field
  must exist with STRING schema type. Search is nonempty and literal (not regex).
  Replace every non-overlapping occurrence, preserving schema, positions and nulls.
  Empty replacement is allowed. Tokens have a 4096-character configuration limit.
- `string-split`: exactly `field`, `separator`, `fields` options. Field must be
  STRING; separator is nonempty literal, at most 4096 characters. `fields` is an
  ordered list of 1..128 nonblank, unique new column names, with no input collisions.
  Preserve original columns and append STRING/nullable columns in supplied order.
  Split at most N-1 times for N output names; last column retains any remainder.
  Trailing empty tokens remain empty strings; missing tokens are null-padded. Null
  input produces all-null appended fields. No unbounded token array allocation.
- Both operators compile field position/schema at construction, reject unknown
  options, wrong option types, unknown fields and incompatible schemas before
  source reading. Rows must match input arity; non-null field values must be Strings.
  Inputs are not mutated; existing cell references are shallow-copied per API.
- Single-thread-confined operators: open once; apply only while open; idempotent
  close, including before open. Existing local chain owns resource cleanup.
- These transforms do not imply SQL expressions, regex, automatic distributed ETL,
  type coercion or JSON-path support. SQL/filter must adopt/pin JSqlParser first;
  JSON-path must use a dedicated library in a later slice.

## Verification and budget

No additional GitHub Actions allocation is available under the conservative ledger.
All commits/PR head use `[skip ci]`; no rerun or workflow-dispatch. PR stays draft,
never claim green CI or merge readiness. Prior unverified-merge exceptions are not
blanket authorization to merge this slice unverified.

Compile production sources locally with JDK17, run real implementation/ServiceLoader
smoke checks, and run the actual local FakeSource -> transforms -> ConsoleSink path.
Supplemental ConsoleSink smoke uses locally available Jackson 2.21 jars rather than
Maven-pinned 2.17.1 and is not a full Maven gate. Add JUnit public-contract and packaged
CLI subprocess E2E tests for eventual verification. Required unrun gates:
`./mvnw -pl seatunnel-transforms-v2 -am test`, CLI `verify`, full reactor `verify`.
Format locally; parse Java/POM XML and check whitespace. Record limits in PR summary.

## Three-line handoff

Next: SQL/filter or JSON-path, with the respective library adopted/pinned first.
Literal string transforms are local row operators, not expression parsers.
Review/merge and reconcile the project-wide CI budget before another automated run.
