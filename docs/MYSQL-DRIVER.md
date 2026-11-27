# MySQL driver ownership, licensing and execution

## Decision

The `jdbc-mysql` adapter uses the official **com.mysql:mysql-connector-j:26.10.0**.
Upstream [tagged LICENSE](https://github.com/mysql/mysql-connector-j/blob/26.10.0/LICENSE)
identifies GPLv2, additional permissions and Universal FOSS Exception1.0. This is
**not a permissive license** and is not classified as Apache-2.0 here. No MariaDB
LGPL driver is silently substituted. This project does not bundle Connector/J in its
default runtime/distribution. Root dependencyManagement records a version policy,
not an active runtime dependency. The explicit `mysql-driver-tests` profile adds it
**test-only** to adapter and CLI tests; shading must still exclude it.

Operators obtain/license the driver and its dependencies separately and retain the
upstream licensing material. The tagged POM declares protobuf-java4.36.1; optional OCI
SDK is not needed for this username/password classic-JDBC adapter. Do not strip required
dependencies from an operator installation. Neither a classpath boundary nor this
packaging choice is a legal determination that GPL/exception/commercial licensing
obligations disappear. Redistribution or incorporation requires your own licensing
assessment. No automatic driver downloader, repository credentials or acceptance on
behalf of the user is provided.

The tagged build targets Java8; the repository runs Java17. Actual artifact loading,
transitive convergence, MySQL server behavior and distribution verification remain
unrun locally. Exact-version GitHub advisory lookup returned no entries, not a full
security audit or a statement about every transitive artifact.

## Operator-owned classpath

After building the CLI in an artifact-enabled environment, obtain the reviewed driver
and its dependencies through your approved process. On Linux/macOS, for example:

```sh
export MYSQL_DRIVER_CLASSPATH=/operator/lib/mysql-connector-j-26.10.0.jar:/operator/lib/protobuf-java-4.36.1.jar
java -cp "seatunnel-cli/target/seatunnel-cli-3.0.0-SNAPSHOT.jar:$MYSQL_DRIVER_CLASSPATH" \
  org.apache.seatunnel.cli.SeaTunnelCommand --config examples/mysql-to-console.conf
```

Paths above are illustrative, not files supplied by this project. Use the platform
classpath separator (`;` on Windows). Standard JVM/Hikari class loading owns driver
resolution; no custom classloader is installed. Plain `java -jar` ignores external
`CLASSPATH` and does not supply MySQL support by itself. Missing driver fails at
runtime with a redacted JDBC error; discovery and config validation do not open IO.

Set `MYSQL_URL=jdbc:mysql://host:3306/database`, `MYSQL_DATABASE=database`, `MYSQL_USER`
and `MYSQL_PASSWORD` via the environment or protected local config. Never commit
credentials or place them in command arguments. URL properties/userinfo/multi-host/
load-balance/failover variants are deliberately rejected. Schema is the MySQL database
and must match the URL path. Identifiers are conservative ASCII, separately backtick
quoted. Both [read](../examples/mysql-to-console.conf) and
[write](../examples/fake-to-mysql.conf) examples require an existing **InnoDB** table.

## Dialect contract

All bounds/lifecycle/INSERT-only batch/rollback/no-restore contracts from
[7d1](SLICE-7D1.md) apply. Driver settings differ intentionally:

- Default `ssl-mode=verify-full` maps to `VERIFY_IDENTITY`; explicit `require` to
  `REQUIRED`, `disable` to `DISABLED`. Configure trust via the driver/JVM trust store.
  No automatic TLS downgrade, public-key retrieval or reconnect; if local auth needs
  TLS, configure TLS instead of silently enabling insecure key retrieval.
- Connect timeout5000ms, socket timeout(query-timeout-seconds+5)*1000ms; query timeout
  remains seconds. These are driver limits, not hard wall-clock/DNS cancellation.
- Server prepared statements and cursor fetch enabled; positive fetch-size limits
  client fetch chunks. No parallel snapshot/fair scheduling/watermarks claim.
- Numeric reads require signed INT/BIGINT/DOUBLE. BOOLEAN reads require **BIT(1)**;
  SQL BOOLEAN/TINYINT(1) aliases are not accepted as booleans. Unsigned/TINYINT/SMALLINT/
  DECIMAL/etc. reads fail rather than widening/coercing. STRING supports textual JDBC
  types. Unsupported types and nonnullable NULLs fail. Sink binds exact incoming Java
  types; external target constraints remain the database's responsibility.
- Runtime checks information_schema.TABLES ENGINE before user IO and rejects views,
  missing tables and non-InnoDB engines. Permissions must allow this metadata lookup.
  Concurrent DDL/engine changes are unsupported; don't race them against a job.
- Explicitly disable multi-queries, batch rewrite/continue-on-error, local-file loading,
  key retrieval and reconnect. Pending InnoDB work rolls back on failure/close; prior
  committed batches remain. No retry/upsert/exactly-once or MyISAM rollback promise.
- Driver NullLogger/paranoid mode and disabled query dumps/profile logging reduce
  diagnostic leakage. Adapter SQLExceptions omit causes/values; packaged CLI suppresses
  Hikari logs. Embedders still own safe pool/dependency logging configuration.

## Required acceptance gates (currently UNRUN)

Provision a disposable MySQL database, then set `PINOT_MYSQL_URL`, `PINOT_MYSQL_USER`,
`PINOT_MYSQL_PASSWORD`; optionally `PINOT_MYSQL_SSL_MODE` (VERIFY_IDENTITY default,
REQUIRED or DISABLED explicitly). Tests use unique table/view names inside that DB and
require CREATE/DROP TABLE, CREATE/DROP VIEW and information_schema visibility. No
container/service is automatically created. PG/HSQLDB evidence does not count as MySQL.

After your driver licensing decision, execute in an artifact-enabled environment:

```sh
./mvnw -Pmysql-driver-tests -pl connector-jdbc -am test
export PINOT_MYSQL_DRIVER_CLASSPATH="$MYSQL_DRIVER_CLASSPATH"
./mvnw -Pmysql-driver-tests -pl seatunnel-cli -am verify
./mvnw -Pmysql-driver-tests -B -ntp verify
```

The CLI test passes **only the packaged CLI plus operator driver classpath** to the
child, not Maven project/test classes. Separate artifact test checks no com/mysql/cj
classes are shaded, including with the test profile. Real service tests are gated on
PINOT_MYSQL_URL: absence means skipped, **not accepted**. Setting URL without driver/
classpath must fail, not silently skip a configured gate. Existing PG/Maven/Hikari/
full-reactor gaps remain. No Actions or external-service spend is authorized by adding
these tests or instructions.
