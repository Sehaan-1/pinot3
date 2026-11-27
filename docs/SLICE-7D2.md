# Slice 7d2 — MySQL JDBC adapter and external-driver policy

## Specification / dependency decision before implementation

Reuse connector-jdbc JDBC source/sink, Hikari ownership, typed bindings, range splits,
batch commit/rollback and unsupported checkpoint/restore contracts from7d1. Introduce
small PostgreSQL/MySQL dialect adaptation for URL/quoting/properties/metadata only;
no pool, protocol, parser, scheduler, codec or retry rewrite. API stays engine-free.
Identifiers jdbc-mysql source/sink. PostgreSQL factories/default constructors retain
previous contracts. MySQL supports bounded reads and INSERT-only writes to existing
transactional InnoDB tables. No MySQL CDC, DDL, upsert or exactly-once claim.

Official com.mysql:mysql-connector-j:26.10.0 reviewed from tagged source: Java8+,
GPLv2 with additional permissions and Universal FOSS Exception1.0, NOT permissive.
Do NOT bundle this driver or add it to default runtime dependencies. Operators obtain
and license the exact driver separately, retaining its license and required dependencies
(tagged POM protobuf-java4.36.1; optional OCI dependency not needed for username/password
classic JDBC). No MariaDB substitution. An explicit mysql-driver-tests Maven profile
adds the exact driver TEST-ONLY to adapter/CLI tests, never to shaded runtime artifacts.
Root dependencyManagement pin alone does not add it. This is a packaging decision,
not a legal determination that linking/classpath use removes GPL obligations; redistributors
must assess the exception/commercial license terms themselves. Full tagged LICENSE
reference documented, not relicensed as Apache. Exact-version GitHub advisory query
returned no entries; not a complete security audit or Maven convergence evidence.

Supported CLI invocation: java -cp seatunnel-cli.jar:<operator driver classpath>
org.apache.seatunnel.cli.SeaTunnelCommand --config job.local.conf. Standard JVM classpath
loading/Hikari driver loading only; no custom classloader/download/credential storage.
The default java -jar mode remains PostgreSQL-capable but has no MySQL driver. A missing
MySQL driver fails at runtime with a redacted JDBC failure, not during factory discovery.

MySQL options match7d1 with dialect-specific URL jdbc:mysql://host[:port]/database,
schema means database/catalog; database must match URL path. No arbitrary URL properties,
userinfo/multi-host/failover/load-balance variants. Conservative names quoted with
backticks, independent of ANSI_QUOTES. ssl-mode defaults verify-full -> VERIFY_IDENTITY;
require -> REQUIRED, disable -> DISABLED. No automatic key retrieval or TLS downgrade.
Set useServerPrepStmts/useCursorFetch, finite connect/socket timeouts (milliseconds),
tinyInt1isBit=false, allowMultiQueries=false, autoReconnect=false, allowPublicKeyRetrieval=false,
rewriteBatchedStatements=false; suppress driver diagnostics including query/value dumps.
BOOLEAN is explicit BIT(1), INT/LONG signed only, DOUBLE and STRING; TINYINT(1)/BOOLEAN
SQL aliases and unsigned numeric mappings rejected on reads to avoid hidden widening/
coercion. Source validates vendor type/precision/signedness as well as JDBC metadata.
Cursor transaction is REPEATABLE_READ/read-only; one reader sequential ranges, no global
parallel snapshot. MySQL snapshot guarantees require transactional InnoDB tables and
appropriate server configuration; no snapshot claim for MyISAM. Same INSERT-only
batch semantics; server nontransactional tables cannot honor rollback and are unsupported.

Tests: dialect/config/quoting/properties/type rejection and resource lifecycle contracts,
unchanged PostgreSQL regressions, real-MySQL gated typed roundtrip/ranges/NULL/duplicate
batch rollback/pending close on InnoDB, gated installed CLI external-classpath roundtrip,
and default artifact exclusion. Proxy/HSQLDB/offline session substitutions explicitly
supplemental, not MySQL/driver/Hikari proof. No Actions; Maven/JUnit/convergence/actual
service gates reported UNRUN if unavailable. User next-then-merge covers this single PR
with disclosed gaps, not later connectors, new CI spend or legal approval on user's behalf.


## Results and gaps

102 executable MySQL dialect/proxy assertions passed: strict URL/dialect/database match,
backtick quoting, TLS mapping/defaults/time units/cursor/security properties, signed/
unsigned/BOOLEAN-width/type metadata, InnoDB vs nontransactional engine check/resource
closure, original source values/NULL, ranged prepared queries, batch commit/rollback/
non-INSERT/driver failure cleanup.825 regression assertions passed (195 JDBC +207
TableMerge +423 token/metadata/serialization/SQL). Both real source-built HSQLDB JDBC
exercises passed, including MySQL-compat mode backticks/prepared batches/rollback/
range/NULL; injected sessions skip actual driver/pool/engine validation integration.
Resource-free ServiceLoader source/sink and both new example factories passed.11 prior
LocalRunner pipelines passed exact rows. Formatter1.19.1/parsing, POM XML/profile scope/
Java headers/local Markdown links/diff checked.

As in7d1, JDK17 compiles adapter code except a temporary JdbcSession copy with only
pool-open replaced by an injection-only UnsupportedOperationException. Production helper
and its new engine-validation call remain uncompiled/unrun locally; existing Hikari
constructor/setter signatures reviewed in tagged7.1.0 source. Properties/metadata/engine
checks tested independently via proxies. Prior temporary shaded/source-built integration
dependencies remain supplemental, not pinned artifacts/convergence evidence.

UNRUN: added/changed JUnit, official Connector/J26.10.0 loading/security/TLS/timeout/
server-cursor behavior, actual MySQL/Hikari lifecycle, gated real InnoDB typed roundtrip/
partition/NULL/duplicate rollback/pending close/reserved identifier/unsigned/view rejection,
installed CLI external-driver roundtrip, default artifact exclusion test, affected Maven/
convergence/full reactor. No Docker/mysql/service or allowed Central access here.
Real-service gates require PINOT_MYSQL_URL, explicit mysql-driver-tests profile and
operator-owned external driver classpath for CLI test; absence is skipped, not accepted.
No automatic service, driver download or Actions. Full PostgreSQL and7b gates remain open.

Driver tagged build/license/POM/property definitions/NullLogger reviewed upstream.
Root version management adds no runtime dependency; profile dependencies TEST only.
GPL/FOSS exception terms not reclassified, no Oracle driver sources/binaries copied
into distribution; legal responsibility isn't erased by this architecture. See
[MYSQL-DRIVER.md](MYSQL-DRIVER.md) for exact operator/classpath/gate instructions.
Concurrent DDL/engine changes unsupported; runtime table-engine preflight is not a lock.

Implemented: MySQL dialect on shared bounded JDBC lifecycle with explicit driver policy.
Pending: real MySQL/ConnectorJ/Hikari/JUnit/Maven/CLI/convergence/artifact/full reactor gates.
Next: streaming/checkpoint runtime prerequisites; JDBC/Stage7 still not fully verified.
