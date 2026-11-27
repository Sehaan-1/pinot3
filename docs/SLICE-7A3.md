# Slice 7a3 — Jayway JSON-path extraction

## Specification and dependency plan

PR title: `[Slice 7a3] Schema-aware Jayway JSON-path extraction`.
Module `seatunnel-transforms-v2`; no API/core/engine changes or dependency cycles.

Adopt `com.jayway.jsonpath:json-path:2.9.0` (Apache-2.0), configured explicitly with
JacksonJsonProvider/JacksonMappingProvider and existing Jackson BOM 2.17.1. Retain
and manage Jayway's internal expression-parser dependencies `net.minidev:json-smart:2.5.2`
and `net.minidev:accessors-smart:2.5.2` (Apache-2.0); do not assume a Jackson input
provider makes json-smart unnecessary. Existing SLF4J 2.0.16 management remains.
Use Jackson for parsing/serialization, Jayway for query compilation/evaluation;
never implement JSON, JSON-path, filter or regex parsers here. Versions are pinned
in root dependency management. SQL/filter transforms remain a separate slice.

## Contracts

- Plugin ID `json-path`, exactly string options `field`, `path`, `target`.
  Input field must exist with STRING type. Target is a new nonblank column name,
  distinct from existing names. Compile the query at construction, before reading.
  Query is the Jayway dialect, not a claim of full RFC 9535 conformance.
- Preserve original columns and append one nullable STRING column. Strings are
  emitted without JSON quotes; other non-null values (numbers, booleans, objects,
  arrays) become compact JSON text. JSON null/missing definite paths append null.
  Indefinite queries produce their normal array result, including `[]` for no match.
  Null source appends null. Input rows/cell references are not mutated/deep-cloned.
- Invalid JSON is an error, not a missing result. Reject duplicate object keys,
  trailing JSON tokens and nesting deeper than 64 via Jackson read constraints.
  Bound input to 1,048,576 UTF-16 code units and paths to 4096 characters. Bounds
  limit parsing/allocation but are not a CPU/time sandbox for recursive/filter/regex
  queries. Only trusted configurations are supported; never expose untrusted paths.
- Explicit Jackson providers per operator, no process-global JsonPath default changes.
  Parse once per row, evaluate compiled query, serialize selected value. Report
  generic evaluation/JSON errors without embedding source data or raw library errors
  in the top-level error message. Preserve the cause for internal debugging.
- Standard single-thread-confined transform lifecycle: one open, apply while open,
  idempotent close (including before open), input arity/runtime String validation.
  Existing local transform chain owns operators; no worker/data-plane claims.

## Verification and budget

Zero Actions jobs: conservative project reservation remains fully allocated.
Commit head uses `[skip ci]`; PR stays draft. Add JUnit public-contract tests and
packaged CLI E2E, but do not claim them run without Maven. Parse Java/XML, format
locally and compile/run supplemental smoke checks if dependencies are available.
Any locally source-built/alternate-version dependencies must be disclosed in PR;
never present that as pinned Maven/CI verification. Required unrun gates:
`./mvnw -pl seatunnel-transforms-v2 -am test`, CLI `verify`, full reactor `verify`.

## Out of scope and handoff

No SQL transform, connector, distributed ETL, arbitrary-code query execution,
JSON mutation, schema inference or automatically flattening arrays into rows.

Next: SQL/filter, adopting and pinning a dedicated parser before implementation.
JSON-path extraction is a local row operator using the Jayway dialect.
Review/merge and reconcile the project-wide Actions budget before automated CI.
