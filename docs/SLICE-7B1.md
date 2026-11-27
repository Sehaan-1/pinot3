# Slice 7b1 — native row-kind metadata and extractor

## Plan / adoption decision (before implementation)

Title: `[Slice 7b1] Native row kinds and metadata-preserving transforms`.
Modules: engine-independent seatunnel-api, transforms-v2; CLI example/E2E and docs.
No new dependency, protocol, scheduler or cycles. Adopt JDK enum/serialization/array
ownership facilities and existing schema/plugin machinery; no external library is
needed for a four-value metadata carrier. Existing Jackson/JUnit/AssertJ remain.
This is the first advanced-transform prerequisite from the full Stage7 program,
not delivery of all advanced transforms, CDC or the remaining connectors.

## API contracts

- New RowKind enum INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE, with stable short strings
  +I/-U/+U/-D respectively. No engine/connector imports or operation inference.
- Existing new SeaTunnelRow(Object...) remains unambiguous and defaults to INSERT,
  including when the first data field is itself a RowKind value. No ambiguous varargs
  constructor overload. Add explicit static ofKind(kind,Object...), getRowKind(),
  withKind(kind) and withFields(Object...) immutable copy APIs.
- Kind is nonnull on construction/copy. Field arrays are cloned, mutable cell values
  remain caller-owned as before. withFields preserves kind; withKind preserves fields.
  getFields/arity remain data-only: metadata does not become a positional column.
- Equality/hash include effective kind and deep field values. Existing default INSERT
  rows retain their previous hash and field-only toString representation. toString
  remains field-only for all kinds, not a changelog wire encoding.
- Retain Java serialVersionUID1. Old snapshots lacking a kind field read as INSERT
  via effective-kind accessor; new snapshots preserve kind. Add a real legacy stream
  fixture generated from the pre-change class plus Java serialization round trips.
  This is narrowly Java old-row compatibility, not durable cross-version checkpoint/
  SDK/engine recovery or a general wire-version compatibility guarantee.

## Transform / extractor contracts

- Existing field mapping/projection, literal replace/split, JSON-path, SQL string/LONG
  append adapters must use withFields so all four kinds survive unchanged. SQL filter
  returns original row as before. Preserve existing schemas/data/null/lifecycle behavior.
- New ServiceLoader provider row-kind-extractor, exactly string option target; target
  must be a new simple ASCII identifier<=128. Append nonnullable STRING containing
  short kind code. Produced schema retains name and prior metadata. Exactly one new
  row per input and preserve native kind. No parsing a payload field or fake metadata.
- Standard single-thread-confined open-once/apply-only-open/idempotent-close lifecycle,
  including close-before-open; reject reopen/null input/wrong arity and target conflicts.
- FakeSource remains INSERT-only; ConsoleSink still prints JSON data fields, not CRUD
  operations. Extraction explicitly exposes kind to output. No claim existing external
  sinks understand deletes/upserts/transactions or that streaming execution now exists.

## Verification / scope / authorization

JUnit API ownership/equality/legacy+new serialization, all kinds across every existing
transform, extractor schema/options/null/lifecycle/discovery. FakeSource -> extractor ->
sql-project -> console example and packaged CLI E2E. Local JDK17 API/operator compile,
legacy stream/readback, metadata-preservation checks and actual LocalRunner examples;
Java formatting/XML/whitespace checks. Report supplemental dependencies separately
from pinned Maven verification. No Actions: grants spent, reserved25m/observed14m45s.
Full affected API/transforms Maven tests, CLI verify and full reactor remain unrun
unless actually executed. User explicitly authorized merge22, next implementation,
then merge that next PR; that is a scoped authorization for this PR only, not blanket
Stage7 merges or CI. No new merge checks are bypassed silently: disclose unrun gates.

Out of scope: FilterToken/TableMerge, multi-input/streaming/checkpoint transaction
runtime, CDC envelope translation/connectors, operation inference and new SQL features.
Next: remaining7b transform/runtime deliverables, then cohesive connector work.


## Local results / verification gaps

JDK17 production API/operator compilation passed.115 executable API/legacy Java
serialization/metadata/operator assertions passed, including old fixture produced by
the actual pre-change SeaTunnelRow class.8 additional JSON-path metadata assertions
passed (all4 kinds).255 existing SQL parser/JDBC regression assertions passed. All9
actual LocalRunner/discovery/FakeSource/ConsoleSink example pipelines passed, exact rows.

Supplemental integration uses tagged source-built parser/HSQLDB/Config1.4.3 and
WireMock3.13.2 shaded Jackson. Temporary out-of-tree JsonFormat AND JsonPathTransform
imports were relinked to shaded Jackson/Jayway for the JSON metadata checks; repo
production imports were not changed. This is not pinned Jayway2.9.0/Jackson2.17.1 or
Maven dependency-convergence/full installed CLI evidence. Classpath installed SQL and
extractor providers only. Added JUnit/discovery/packaged CLI E2E, affected API/transforms
Maven tests, full reactor remain unrun. Maven Central outside outbound allowlist.
Changed Java formatting/parsing (google-java-format1.19.1), local Markdown links,
POM XML and diff whitespace passed. No Actions launched. Explicit scoped user merge
instruction is not a green-build claim or blanket future permission.
