# Slice 7d1 — bounded PostgreSQL JDBC source and sink

## Specification and adoption decision (before implementation)

One cohesive connector-jdbc module, installed source/sink identifier jdbc-postgres.
Adopt com.zaxxer:HikariCP:7.1.0 (Apache-2.0, Java11+) and
org.postgresql:postgresql:42.7.15 (BSD-style PostgreSQL license), exact central pins.
Use JDBC prepared statements/typed setters and pgjdbc cursor fetch; never implement a
pool, wire protocol, SQL parser, scheduler or retry engine. Align root SLF4J to2.0.17
(Hikari's declared version). GitHub tagged POM/licenses checked. GitHub advisory API
queries for these exact versions returned no entries at implementation entry; this is
not a comprehensive/transitive security audit. Maven convergence still must be checked.

Strict options: url (postgresql network URL without query/userinfo), user, password,
schema (SQL schema name), table, columns (1..128 name/type/nullable maps for source),
fetch-size (source,1..10000,default500), batch-size (sink,1..1000,default100),
query-timeout-seconds (1..300,default30), ssl-mode (verify-full default, require or disable
explicitly). Source optional partition-column, lower-bound, upper-bound, partitions:
all-or-none, INT/LONG selected column, half-open [lower,upper),2..32 disjoint ranges.
No SQL fragments: conservative identifiers quoted individually; table/schema <=63 ASCII
identifier chars. No URL properties; credentials separate; no secret-bearing toString.
Factories validate without IO. Errors from database operations are sanitized without
retaining driver causes that might expose credentials/row values.

Types supported: BOOLEAN, INT, LONG, DOUBLE, STRING only; exact Java representations,
strict NULL/arity/nonnullable validation. Source validates actual JDBC column types,
no coercion from numeric/text lookalikes. Sink uses incoming CatalogTable, rejects
unsupported types and non-INSERT kinds. No DDL, arbitrary query, upsert or MySQL claim.
Source reads explicit columns, no ORDER BY guarantee, bounded one-reader enumeration
of immutable range splits; optional partitions cover only requested range, excluding
NULL partition values. Unpartitioned mode includes all rows. One reader transaction
(REPEATABLE_READ/read-only) and cursor, not a global cross-reader database snapshot.
Advance only after downstream accepts row; output failure makes reader terminal, no
in-process retry. No checkpoint/restore/split-return support: reject explicitly.

Runtime owner opens pool(max1)/connection; reader start or writer creation only.
Fetch-size/query/connect/socket/pool timeouts finite; synchronous pgjdbc calls bounded
by driver timeouts, not immediate asynchronous cancellation. Poll emits at most one
row. Source closes resultset/statement per split, connection/pool on failure/close.
Writer buffers driver batch <=batch-size, flushes+commits each batch and final flush;
rollback pending batch on failure/close; close never commits. Prior committed batches
remain on later failure. No retries, exactly-once or external committer. Ambiguous
commit outcome requires operator reconciliation. Thread-confined one-shot lifecycle.

Tests: config/schema/typing/NULL/kinds/splits/lifecycle/resource failure contracts,
real PostgreSQL gated roundtrip + batch failure/rollback and source partition reads;
installed CLI dependency and service descriptors plus gated packaged CLI E2E. Local
source-built/shaded/HSQLDB checks are supplemental, not PostgreSQL substitutes. Required
affected-module Maven/JUnit, PostgreSQL and full reactor checks reported UNRUN if no
service/artifact access. No Actions authorized; user next-then-merge applies this PR
only despite disclosed verification gaps. Existing 7b verification gaps remain open.


## Results / open gates

195 checked-in executable proxy/config assertions passed under JDK17: strict options/
URL/redaction/identifiers/types/nulls/split bounds (including signed64 overflow-safe
partitioning), lifecycle/foreign splits/restore rejection, source output failure,
statement/query/metadata/read/close failures, typed batch setters/nulls, commit/rollback/
pending-close, non-INSERT rejection and partial writer startup cleanup.
Real source-built HSQLDB JDBC exercise passed prepared batch/commit/rollback/NULL and
partitioned cursor reads; not a PostgreSQL substitute. Resource-free ServiceLoader
providers and both example factories passed, with supplied placeholder config values.
630 existing assertions (207 TableMerge +423 token/metadata/SQL) and11 prior real
LocalRunner example pipelines passed. Formatter1.19.1/parsing, POM/logback XML, Java
license headers, Markdown local links and diff whitespace checked.

Offline compilation used the real adapter code EXCEPT a temporary JdbcSession copy
whose pool-opening method throws UnsupportedOperationException, allowing package-private
injected JDBC sessions in tests. No Hikari API stubs or pool rewrites committed. Actual
production Hikari opening method is uncompiled/unrun locally; tagged7.1.0 constructor/
setter signatures reviewed. Previous shaded-Guava/Jackson/Jayway/source-built Config/
parser/HSQLDB integration dependencies retained. This is NOT pinned dependency proof.
GitHub pgjdbc release download redirects to blocked release-assets host and failed;
Maven Central outside allowlist; Docker/psql/PostgreSQL service not present.

Added JUnit contracts/runtime-class/discovery test, real PostgreSQL typed roundtrip/
range cursor/duplicate-batch rollback/pending-close test, installed CLI PostgreSQL roundtrip
and pool-log redaction regression are UNRUN through Maven. Real PostgreSQL, pgjdbc,
Hikari pool lifecycle/timeouts/TLS, dependency convergence/installed packaging and full
reactor remain UNRUN. Real-service tests explicitly gate on PINOT_PG_URL, and skipped
tests do not count as passing acceptance. No library runtime guarantees inferred from
proxies or HSQLDB. Full7b verification gaps also remain.

pgjdbc's tagged build declares checker-qual3.55.1 (aligned centrally against Guava).
Bundled SCRAM implementation is driver-owned/shaded, not a new handwritten auth layer.
CLI suppresses Hikari logs that may contain raw driver exceptions; pgjdbc property
logServerErrorDetail=false confirmed in tagged source. Embedders still own dependency
logger configuration; no arbitrary logging redaction promise. Driver timeouts are not
hard wall-clock cancellation, notably around DNS. PostgreSQL license retained in
META-INF/LICENSE-postgresql.txt. No MySQL driver silently adopted.

Implemented: bounded PostgreSQL JDBC source/sink with explicit supported contracts.
Pending: real PostgreSQL/Hikari/pgjdbc/Maven/JUnit/convergence/installed CLI/full reactor.
Next: JDBC verification and MySQL license/packaging decision; Stage7 remains incomplete.
