# Slice 7b3 — bounded TableMerge and multi-input local execution

## Specification before implementation

Complete the remaining 7b implementation with a bounded, schema-aligned UNION ALL.
Adopt existing LocalRunner, source/enumerator SPI, TransformChain and JDK try-with-
resources. No new scheduler, tokenizer, serialization library or dependencies required.
TableMerge is a core topology operation, not a fake unary transform plugin.

Config accepts exactly one of legacy source or sources (2..32 plugin/options entries).
All inputs must be bounded and have identical ordered columns (name/type/nullability);
table identifiers may differ. Output schema/identifier comes from the first input.
Visit inputs serially in configuration order; preserve duplicate rows and row kinds.
One shared transform chain and sink writer, one final flush. Downstream synchronous
writes provide backpressure; no intermediate row buffer. Each reader/enumerator closes
before the next input starts, including failures. Failure aborts later inputs, closes
writer/chain, and never performs success flush. Interrupted polling aborts as before.
No join, fairness, watermarks, streaming, checkpoint/restore or transactional guarantees.
Factories remain resource-free by SPI contract. Reject schema/boundedness mismatch
before opening runtime resources. Keep existing JobConfig constructor signatures.

Tests: legacy parsing, mutually exclusive/empty/oversized inputs, schema mismatch,
ordered duplicate-preserving union, shared transform/sink lifecycle and failure cleanup.
Real multi-FakeSource -> transform -> ConsoleSink E2E plus installed CLI regression.
Local supplemental JDK17 compilation/execution allowed, with dependency substitutions
explicitly disclosed. Maven/JUnit/full reactor/installed CLI gates remain unrun unless
actually executed. No Actions authorized. User requested all remaining 7b and merge;
that is scoped to this cohesive implementation, not new CI spend or connectors.


## Results / verification gaps

207 executable assertions passed from the same checked-in contract driver invoked by
JUnit: all row kinds/original identity/duplicates, serial resource ownership, shared
writer/final flush, empty inputs/32 boundary, NOTHING_AVAILABLE, schema/name/type/
nullability/unbounded rejection before resources, partial startup/poll/write/close/
second-input arity failure, interruption, config shape/exclusivity/bounds/ownership.
423 existing token/metadata/serialization/SQL regression assertions also passed.
Eleven actual LocalRunner pipelines passed with exact expected output: new three-input
FakeSource -> FilterToken -> RowKindExtractor -> ConsoleSink plus ten existing examples.

JDK17 compiles real changed core/config without substitutions. As previously, integration
uses out-of-tree Guava/Jackson import relinks to WireMock3.13.2 shaded dependencies and
source-built Config1.4.3/JSqlParser/HSQLDB. Selected provider set only. Not pinned artifact
convergence or installed CLI evidence. Formatter1.19.1 (not pinned1.19.2), POM XML/local
Markdown links/diff checked. Added JUnit and packaged CLI test are UNRUN via Maven;
affected-module Maven/full reactor gates also UNRUN. No Actions started.

No new dependency or engine-specific API imports. All named7b features (native kinds/
extractor, token filter, bounded TableMerge) implemented; full verification still pending.
Programmatic executeTableMerge accepts1..32 sources for single-input reuse; HOCON
sources requires2..32. No distributed multi-input topology/checkpoint runtime claim.

Implemented: remaining bounded TableMerge and multi-source runtime with failure cleanup.
Pending: Maven/JUnit, installed CLI and full-reactor gates across7b.
Next: JDBC; streaming runtime remains a prerequisite for later unbounded connectors.
