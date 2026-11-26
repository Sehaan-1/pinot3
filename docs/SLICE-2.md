# Slice 2 — Common & Config

PR: `[Slice 2] seatunnel-common + seatunnel-config (exceptions, utils, HOCON config)`

## Dependency plan and adoption decisions

- `seatunnel-common` owns `org.apache.seatunnel.common.exception` and `.utils`.
- `seatunnel-config` owns `org.apache.seatunnel.config` and depends on common;
  common never depends on config. Neither module imports engine or connector code.
- Adopt Jackson (`com.fasterxml.jackson.core:jackson-databind`, BOM
  `com.fasterxml.jackson:jackson-bom:2.17.1`) for JSON; build only a small exception-
  translating facade, not a JSON parser.
- Adopt `com.typesafe:config:1.4.3` for HOCON, substitutions and layered overrides;
  build only the typed facade and builder, not a configuration parser.
- Use `org.slf4j:slf4j-api:2.0.16` with
  `ch.qos.logback:logback-classic:1.5.6` runtime wiring.
- Foundation dependencies inherited from the root: `com.google.guava:guava:33.2.1-jre`,
  `org.apache.commons:commons-lang3:3.17.0`,
  `org.apache.commons:commons-collections4:4.4`, `commons-io:commons-io:2.16.1`.
- Tests use `org.junit.jupiter:junit-jupiter:5.11.0` and
  `org.assertj:assertj-core:3.26.3`.

These libraries use permissive licenses. Thin map accessors and Java reflection
helpers are project-specific adapters; no custom discovery mechanism is introduced.

## Contracts

- Unchecked `SeaTunnelException`, `ConfigException`, `JsonException` preserve causes.
- `JsonUtils` shares a private Jackson mapper; callers cannot mutate its configuration.
- `ReflectionUtils` translates loading and construction failures into common exceptions.
- `MapUtils` provides typed fallback accessors for string-keyed maps.
- `Config` exposes typed reads, defaults, nested configs and required-key validation;
  Typesafe failures are translated into the common `ConfigException`.
- `ConfigBuilder` is mutable and single-threaded. Overrides win over the loaded source;
  when multiple source kinds are supplied the precedence is file, resource, string.

## Out of scope

Engine-independent Source/Transform/Sink SPI, discovery, formats, CLI, execution,
metrics and connectors belong to later slices. No runnable ETL pipeline is claimed.

## Verification

```sh
./mvnw -pl seatunnel-common,seatunnel-config -am test
./mvnw -B -ntp verify
```

The full gate includes unit tests, Spotless, strict Java license-header checks and
JaCoCo reports. The agent sandbox has no JDK and cannot reach Maven Central;
executable verification runs on GitHub Actions (Temurin 17).

## Three-line handoff

Next: Slice 3 introduces `seatunnel-api`, depending only on foundation modules.
Define row/schema/options and Source/Transform/Sink/state contracts without engine imports.
Verify `./mvnw -pl seatunnel-api -am test`; do not start until Slice 2 is reviewed and merged.
