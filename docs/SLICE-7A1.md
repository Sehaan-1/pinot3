# Slice 7a1 — Field rename/copy transforms and local transform chains

## Specification and dependency plan

PR title: `[Slice 7a1] Schema-aware field transforms and local transform chains`.
Modules: new `seatunnel-transforms-v2`; `seatunnel-plugin-discovery` adds TransformPlugin;
`seatunnel-core` adds optional transform chain; CLI packages providers and E2E tests.
Dependencies: transforms -> API + plugin discovery, core -> shared discovery SPI,
CLI -> transform implementation. No API -> engine/implementation imports or cycles.

Build/adopt: reuse JDK ServiceLoader, existing plugin discovery, Typesafe Config
1.4.3 for HOCON, existing API schema/row types, and JDK try-with-resources for ownership.
Field-name resolution and positional row mappings are engine/domain logic, not a new
parser or expression evaluator. No new library coordinates. Reuse JUnit 5.11.0,
AssertJ 3.26.3, Surefire/Failsafe 3.2.5 and existing formatter/license/JaCoCo gates.

Roadmap adjustment: split the broad original 7a row into independently reviewed slices.
This first PR delivers only rename/copy plus end-to-end runtime integration. SQL/filter
must adopt and pin JSqlParser before implementation; JSON-path must adopt a dedicated
library. Do not invent SQL, expression, regex or JSON-path parsers in this slice.

## Contracts

- `transforms` is an optional ordered HOCON list, at most 32 stages. Entries contain
  `plugin` and optional `options`, no unknown keys. The old four-argument JobConfig
  constructor and configs without transforms retain behavior. Factories receive the
  preceding stage's schema; the sink receives the final output schema.
- `field-rename` options: `fields={oldName=newName}`. Renames are simultaneous, preserving
  field positions, data types, nullability and table name. Unknown source names, empty
  mappings, blank targets and duplicate final names reject construction.
- `field-copy` options: `fields={newName=existingName}`. Append copies in lexical target
  name order, preserving existing columns. Sources refer only to the input schema,
  never newly appended fields. Unknown sources and name collisions reject construction.
- Both mappings have at most 128 entries, case-sensitive exact names and string values,
  with no other option keys. Input rows are not mutated. Field references are copied
  shallowly, matching SeaTunnelRow; no claim of deep cloning mutable cell payloads.
- Operators are single-thread-confined: one open, apply only while open, idempotent
  close (also safe before open). Arity mismatches fail clearly. API instances are real
  implementations, not placeholders. Source order and transform list order are preserved.
- Local runtime owns created transforms, opens in order, closes in reverse on success,
  factory/open/row/sink failure and partial startup. Chains permit filtering/fan-out per
  existing SPI (empty/multiple row lists), validate every stage's output arity and reject
  null rows/lists. Return count is emitted sink rows. No whole-input materialization.
- Existing source/sink lifecycle and local-only parallelism=1 remain intact. The engine
  still does not install distributed connector operators or route row data remotely.

## Verification and budget

Compile API + transform implementation and execute a dependency-free local Java smoke
harness with the available JDK17 before CI. Format locally and check Java/XML/whitespace.
Add public-contract JUnit tests, chain lifecycle/failure tests and packaged CLI E2E
FakeSource -> rename -> copy -> ConsoleSink plus invalid mapping/schema cases.

Affected gates: `./mvnw -pl seatunnel-transforms-v2,seatunnel-core -am test`, CLI
`verify`, full reactor `verify`. At most ONE five-minute Actions run may use the
remaining project-wide reservation in `docs/CI-BUDGET.md`, after local checks; no
exploratory pushes/reruns. CI also checks the previously unverified Slice 6d code.
A failed run is a blocked draft, never permission to silently spend more minutes.
User explicitly merged 6d without verification; that exception is not a claim its
gates passed and is not blanket permission to merge future unverified slices.

## Out of scope

SQL/filter evaluation, replace/split/JSON-path, advanced transforms, distributed ETL,
checkpoint wiring, new connectors and automatic task replay.

## Three-line handoff

Next: remaining core transforms, each with an explicit parser/library adoption plan.
Local transform chains do not close the engine's distributed row-routing gaps.
Start after review/merge; no project Actions budget resets between sessions.
