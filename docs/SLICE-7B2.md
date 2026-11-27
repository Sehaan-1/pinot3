# Slice 7b2 — exact-token row filtering

## Specification / adoption decision (before implementation)

Title: `[Slice 7b2] Literal token filters with row-kind preservation`.
Production module seatunnel-transforms-v2, plus tests/CLI/example/docs. Adopt existing
centrally pinned com.google.guava:guava:33.2.1-jre (Apache-2.0) as an explicit direct
module dependency and use Splitter.on(String) lazy iteration. Reuse existing strict
StringOptions adaptation and JDK Set membership; no regex/custom tokenizer, parser,
protocol, scheduler, API/core/engine import or dependency cycle.

## Contracts

- ServiceLoader plugin filter-token; exactly options field, delimiter, tokens, mode.
  field selects one existing exact-case STRING column. mode is exactly lower-case
  any or all; public API enum ANY/ALL. Schema/table identity unchanged.
- delimiter is a nonempty literal String<=128 UTF-16 units, never regex. tokens is
  a list of1..128 distinct nonempty strings, each<=4096 UTF-16 units. Reject duplicate/
  empty/null/nonstring tokens, invalid option sets/types/mode/field/schema at factory
  construction. Copy requested tokens so subsequent caller mutation cannot change it.
- Split exactly using Guava's literal, non-overlapping delimiter semantics. No trimming,
  normalization, case folding, quote/CSV handling or substring matching. Empty segments
  cannot match because requested empty tokens are rejected. Repeated data tokens do
  not satisfy multiple different requested tokens; this is set membership, not counts.
- ANY keeps when at least one requested token occurs; ALL keeps when every requested
  token occurs, independent of order. Return the original row object (including native
  row kind and mutable-cell references), or no rows. Never modify row fields/metadata.
- Selected source null drops in both modes, even if schema marks it nonnullable.
  Non-null selected value must be String; other input cell types are opaque. Require
  exact row arity. Source string<=1,048,576 UTF-16 units; oversized values fail even
  if the first token would match. Token iteration is lazy; at most128 matched-token
  entries retained. Short-circuit after a successful ANY/ALL match. No split array or
  unbounded per-row set of unrelated tokens. Input cap is not a hostile CPU sandbox.
- Standard single-thread-confined open-once/apply-only-open/idempotent-close lifecycle,
  including close-before-open; reject null row, reopen, wrong arity. No external resources.
  Filter respects all INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE kinds without inferring
  new changes; filtering CDC before/after images independently can affect changelog
  consistency and must be chosen knowingly. No relational retraction reconciliation.

## Verification / scope / authorization

JUnit exact/literal/multi-char/ANY/ALL/duplicates/whitespace/Unicode/NULL/bounds/ownership/
metadata/options/lifecycle/discovery contracts. Real FakeSource -> filter-token ->
ConsoleSink example and packaged CLI E2E. Local JDK17 operator compile and executable
smoke/LocalRunner regressions with available adopted libraries; disclose temporary
shaded Guava/Jackson dependency relinks, not pinned Maven evidence. Maven API/transforms
checks, CLI verify and full reactor remain unrun unless actually executed. Source API
signature review of pinned Guava supplements but does not replace dependency verification.
Format Java, parse POM XML and Markdown links, git diff --check. No Actions: prior grants
spent, reserved25m/observed14m45s unchanged. User authorized next then merge for this
single PR, a scoped unverified-merge exception, not future merge/CI permission.

Out of scope: native row-kind changes, regex/fuzzy/CSV tokenization, TableMerge/multi-
input/streaming/checkpoint runtime, connectors and CDC reconciliation. Row-kind carrier/
extractor already landed in PR23. Remaining Stage7 work is not marked complete.


## Local results and three-line handoff

43 executable exact-token assertions passed: ANY/ALL, exact-vs-substring, literal/multi-
character delimiters, whitespace/case, Unicode, NULL/empty segments, config mutation,
type/arity/size rejection and original-row/all-kind preservation.380 regression
assertions passed:115 native-kind/legacy+8 JSON metadata,255 SQL parser/JDBC and2 legacy
StringOptions.fields adaptation checks. All10 actual LocalRunner/discovery/FakeSource/
ConsoleSink example pipelines passed, exact expected rows.

JDK17 compiled the adapter from a temporary copy with only its Guava import relinked
to WireMock3.13.2 shaded Guava (and existing out-of-tree JsonFormat/JsonPathTransform
Jackson/Jayway relinks). Production repo imports remain com.google/com.fasterxml/
com.jayway. Pinned Guava v33.2.1 source confirms Splitter.on(String)/split(CharSequence)
API, but actual guava33.2.1 Maven artifact/convergence is not verified locally. Existing
tagged source-built parser/HSQLDB/Config1.4.3 used. Supplemental provider registry
contains SQL/extractor/filter-token providers, not every unrelated provider.
Changed Java formatting/parsing (google-java-format1.19.1), POM XML, Markdown links and
diff whitespace passed. Added/changed JUnit/discovery/packaged CLI, affected Maven and
full reactor unrun (Central outside outbound allowlist). No Actions/green-build claim.

Implemented: strict literal ANY/ALL token membership preserving native row kinds.
Pending: API/transforms Maven/JUnit, packaged CLI and full reactor verification.
Next: remaining multi-input/TableMerge/runtime prerequisites, then connectors; no
blanket merge or CI authorization implied by the current scoped merge instruction.
