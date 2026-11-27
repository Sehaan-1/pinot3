/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Forks the packaged executable JAR: no in-process mocks, classpath shortcuts or core stubs. */
class PipelineIT {
  @TempDir Path temporary;
  private int sequence;

  private record Result(int code, String output, String errors) {}

  private Result run(String configText, String... extra) throws Exception {
    return runWithDriver(null, configText, extra);
  }

  private Result runWithDriver(String driverClasspath, String configText, String... extra)
      throws Exception {
    Path folder = Files.createDirectory(temporary.resolve("run-" + sequence++));
    var args = new ArrayList<String>();
    args.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    String jar = Path.of("target", "seatunnel-cli-3.0.0-SNAPSHOT.jar").toAbsolutePath().toString();
    if (driverClasspath == null) args.addAll(List.of("-jar", jar));
    else
      args.addAll(
          List.of(
              "-cp",
              jar + java.io.File.pathSeparator + driverClasspath,
              "org.apache.seatunnel.cli.SeaTunnelCommand"));
    if (configText != null) {
      Path config = folder.resolve("job.conf");
      Files.writeString(config, configText);
      args.addAll(List.of("--config", config.toString()));
    }
    args.addAll(List.of(extra));
    Path stdout = folder.resolve("stdout.txt");
    Path stderr = folder.resolve("stderr.txt");
    Process process =
        new ProcessBuilder(args)
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("CLI process completed").isTrue();
      return new Result(process.exitValue(), Files.readString(stdout), Files.readString(stderr));
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly().waitFor();
      }
    }
  }

  @Test
  void defaultExecutableNeverBundlesMysqlDriverEvenWithTestProfile() throws Exception {
    try (var jar =
        new java.util.jar.JarFile(Path.of("target", "seatunnel-cli-3.0.0-SNAPSHOT.jar").toFile())) {
      assertThat(jar.stream().noneMatch(e -> e.getName().startsWith("com/mysql/cj/"))).isTrue();
    }
  }

  @Test
  @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(
      named = "PINOT_MYSQL_URL",
      matches = ".+")
  void packagedMysqlRoundTripUsesOperatorSuppliedDriverClasspath() throws Exception {
    String driver = System.getenv("PINOT_MYSQL_DRIVER_CLASSPATH");
    assertThat(driver)
        .as("Provide operator-owned Connector/J and dependencies classpath")
        .isNotBlank();
    String url = System.getenv("PINOT_MYSQL_URL");
    String schema = java.net.URI.create(url.substring(5)).getPath().substring(1);
    String table = "test_cli_" + java.util.UUID.randomUUID().toString().replace("-", "");
    var properties = new java.util.Properties();
    properties.setProperty("user", System.getenv("PINOT_MYSQL_USER"));
    properties.setProperty("password", System.getenv("PINOT_MYSQL_PASSWORD"));
    String ssl = System.getenv().getOrDefault("PINOT_MYSQL_SSL_MODE", "VERIFY_IDENTITY");
    properties.setProperty("sslMode", ssl);
    properties.setProperty("connectTimeout", "5000");
    properties.setProperty("socketTimeout", "35000");
    properties.setProperty("allowPublicKeyRetrieval", "false");
    String mode =
        switch (ssl) {
          case "VERIFY_IDENTITY" -> "verify-full";
          case "REQUIRED" -> "require";
          case "DISABLED" -> "disable";
          default -> throw new IllegalArgumentException("Invalid MySQL test sslMode");
        };
    String options =
        "url=${PINOT_MYSQL_URL}\n"
            + "user=${PINOT_MYSQL_USER}\n"
            + "password=${PINOT_MYSQL_PASSWORD}\n"
            + "schema="
            + schema
            + "\ntable="
            + table
            + "\nssl-mode="
            + mode
            + "\n";
    try (var connection = java.sql.DriverManager.getConnection(url, properties);
        var ddl = connection.createStatement()) {
      ddl.execute(
          "CREATE TABLE `" + table + "` (id BIGINT PRIMARY KEY,name TEXT NOT NULL) ENGINE=InnoDB");
      try {
        Result written =
            runWithDriver(
                driver,
                "source {plugin=fake,options={rows=3,start=10}}\n"
                    + "sink {plugin=jdbc-mysql,options {\n"
                    + options
                    + "batch-size=2\n}}");
        assertThat(written.code()).as(written.errors()).isZero();
        assertThat(written.output()).isEmpty();
        Result read =
            runWithDriver(
                driver,
                "source {plugin=jdbc-mysql,options {\n"
                    + options
                    + "columns=[{name=id,type=LONG,nullable=false},{name=name,type=STRING,nullable=false}]\n"
                    + "fetch-size=1\n"
                    + "}}\n"
                    + "sink.plugin=console");
        assertThat(read.code()).as(read.errors()).isZero();
        assertThat(read.output().lines().toList())
            .containsExactlyInAnyOrder("[10,\"row-10\"]", "[11,\"row-11\"]", "[12,\"row-12\"]");
      } finally {
        ddl.execute("DROP TABLE `" + table + "`");
      }
    }
  }

  @Test
  @org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable(
      named = "PINOT_PG_URL",
      matches = ".+")
  void packagedPostgresRoundTripUsesInstalledDriverAndProviders() throws Exception {
    String schema = "test_cli_" + java.util.UUID.randomUUID().toString().replace("-", "");
    var properties = new java.util.Properties();
    properties.setProperty("user", System.getenv("PINOT_PG_USER"));
    properties.setProperty("password", System.getenv("PINOT_PG_PASSWORD"));
    properties.setProperty(
        "sslmode", System.getenv().getOrDefault("PINOT_PG_SSL_MODE", "verify-full"));
    properties.setProperty("connectTimeout", "5");
    properties.setProperty("socketTimeout", "35");
    String options =
        "url=${PINOT_PG_URL}\nuser=${PINOT_PG_USER}\npassword=${PINOT_PG_PASSWORD}\n"
            + "schema="
            + schema
            + "\ntable=events\nssl-mode="
            + properties.getProperty("sslmode")
            + "\n";
    try (var connection =
            java.sql.DriverManager.getConnection(System.getenv("PINOT_PG_URL"), properties);
        var ddl = connection.createStatement()) {
      ddl.execute("CREATE SCHEMA \"" + schema + "\"");
      try {
        ddl.execute(
            "CREATE TABLE \"" + schema + "\".events (id BIGINT PRIMARY KEY, name TEXT NOT NULL)");
        Result write =
            run(
                "source {plugin=fake,options={rows=3,start=10}}\n"
                    + "sink {plugin=jdbc-postgres,options {\n"
                    + options
                    + "batch-size=2\n}} ");
        assertThat(write.code()).as(write.errors()).isZero();
        assertThat(write.output()).isEmpty();
        Result read =
            run(
                "source {plugin=jdbc-postgres,options {\n"
                    + options
                    + "columns=[{name=id,type=LONG,nullable=false},{name=name,type=STRING,nullable=false}]\n"
                    + "fetch-size=1\n"
                    + "}}\n"
                    + "sink.plugin=console");
        assertThat(read.code()).as(read.errors()).isZero();
        assertThat(read.output().lines().toList())
            .containsExactlyInAnyOrder("[10,\"row-10\"]", "[11,\"row-11\"]", "[12,\"row-12\"]");
      } finally {
        ddl.execute("DROP SCHEMA \"" + schema + "\" CASCADE");
      }
    }
  }

  @Test
  void packagedPipelineMergesMultipleInputsBeforeSharedTransforms() throws Exception {
    Result result =
        run(
            "sources=[{plugin=fake,options={rows=2,start=10}},{plugin=fake,options.rows=0},{plugin=fake,options={rows=2,start=11}}]\n"
                + "transforms=[{plugin=filter-token,options={field=name,delimiter=\"-\",tokens=[\"11\",\"12\"],mode=any}},{plugin=row-kind-extractor,options.target=change}]\n"
                + "sink.plugin=console");
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.errors()).isEmpty();
    assertThat(result.output().lines().toList())
        .containsExactly(
            "[11,\"row-11\",\"+I\"]", "[11,\"row-11\",\"+I\"]", "[12,\"row-12\",\"+I\"]");
  }

  @Test
  void packagedJarRunsTheRealPipelineWithExactDeterministicRows() throws Exception {
    Result result =
        run(
            "env.parallelism=1\n"
                + "source { plugin=fake, options { rows=5, start=10, split-size=2 } }\n"
                + "sink.plugin=console",
            "--mode",
            "local");
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.errors()).isEmpty();
    assertThat(result.output().lines().toList())
        .containsExactly(
            "[10,\"row-10\"]",
            "[11,\"row-11\"]",
            "[12,\"row-12\"]",
            "[13,\"row-13\"]",
            "[14,\"row-14\"]");
  }

  @Test
  void zeroRowsIsSuccessfulEmptyOutput() throws Exception {
    Result result = run("source { plugin=fake, options.rows=0 }\nsink.plugin=console");
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output()).isEmpty();
  }

  @Test
  void errorsProduceNonzeroExitsAndNoData() throws Exception {
    for (String config :
        List.of(
            "source.plugin=missing\nsink.plugin=console",
            "source { plugin=fake, options.rows=-1 }\nsink.plugin=console",
            "source { plugin=fake, options.typo=1 }\nsink.plugin=console",
            "source.plugin=fake\nsink { plugin=console, options.typo=true }",
            "source = [invalid")) {
      Result result = run(config);
      assertThat(result.code()).isEqualTo(1);
      assertThat(result.output()).isEmpty();
      assertThat(result.errors()).contains("Job failed:");
    }
  }

  @Test
  void stdoutDeviceFailureIsNotSilentlyReportedAsSuccess() throws Exception {
    Path full = Path.of("/dev/full");
    org.junit.jupiter.api.Assumptions.assumeTrue(Files.exists(full), "Requires /dev/full");
    Path config = temporary.resolve("output-failure.conf");
    Files.writeString(config, "source.plugin=fake\nsink.plugin=console");
    Path errors = temporary.resolve("output-failure.txt");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar",
                Path.of("target", "seatunnel-cli-3.0.0-SNAPSHOT.jar").toAbsolutePath().toString(),
                "--config",
                config.toString())
            .redirectOutput(full.toFile())
            .redirectError(errors.toFile())
            .start();
    try {
      assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue();
      assertThat(process.exitValue()).isEqualTo(1);
      assertThat(Files.readString(errors)).contains("standard output write failed");
    } finally {
      if (process.isAlive()) {
        process.destroyForcibly().waitFor();
      }
    }
  }

  @Test
  void helpVersionAndUsageAreAvailableFromThePackagedArtifact() throws Exception {
    assertThat(run(null, "--help").code()).isZero();
    assertThat(run(null, "--version").output()).contains("SeaTunnel local baseline");
    assertThat(run(null).code()).isEqualTo(2);
    assertThat(run("source.plugin=fake\nsink.plugin=console", "--mode", "cluster").code())
        .isEqualTo(2);
  }

  @Test
  void packagedPipelineChainsRenameThenCopyUsingEvolvedSchema() throws Exception {
    Result result =
        run(
            "source { plugin=fake, options { rows=2, start=10 } }\nsink.plugin=console\n"
                + "transforms=[{plugin=field-rename,options.fields={id=event_id}},"
                + "{plugin=field-copy,options.fields={copied_id=event_id}}]");
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[10,\"row-10\",10]", "[11,\"row-11\",11]");
  }

  @Test
  void invalidTransformSchemaFailsBeforeAnyRowsAreWritten() throws Exception {
    Result result =
        run(
            "source.plugin=fake\nsink.plugin=console\n"
                + "transforms=[{plugin=field-copy,options.fields={copied=missing}}]");
    assertThat(result.code()).isEqualTo(1);
    assertThat(result.output()).isEmpty();
    assertThat(result.errors()).contains("Unknown source field");
  }

  @Test
  void packagedPipelineReplacesThenSplitsWithLiteralDelimiter() throws Exception {
    Result result =
        run(
            "source { plugin=fake, options { rows=2, start=10 } }\n"
                + "sink.plugin=console\n"
                + "transforms=[{plugin=string-replace,options={field=name,search=\"-\",replacement=\"|\"}},"
                + "{plugin=string-split,options={field=name,separator=\"|\",fields=[prefix,number]}}]");
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[10,\"row|10\",\"row\",\"10\"]", "[11,\"row|11\",\"row\",\"11\"]");
  }

  @Test
  void invalidStringTransformTypeFailsBeforeSourceReading() throws Exception {
    Result result =
        run(
            "source.plugin=fake\nsink.plugin=console\n"
                + "transforms=[{plugin=string-replace,options={field=id,search=a,replacement=b}}]");
    assertThat(result.code()).isEqualTo(1);
    assertThat(result.output()).isEmpty();
    assertThat(result.errors()).contains("STRING type");
  }

  @Test
  void packagedPipelineExtractsJsonUsingInstalledProvider() throws Exception {
    Result result = run(Files.readString(Path.of("..", "examples", "fake-json-path-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .as("JSON-only stdout: %s", result.output())
        .hasSize(1);
    var row = new com.fasterxml.jackson.databind.ObjectMapper().readTree(result.output().trim());
    assertThat(row.size()).isEqualTo(3);
    assertThat(row.get(0).asLong()).isEqualTo(10L);
    assertThat(row.get(2).asText()).isEqualTo("Ada");
  }

  @Test
  void packagedPipelineFiltersWithInstalledSqlProvider() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-filter-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[12,\"row-12\"]", "[14,\"row-14\"]");
  }

  @Test
  void packagedPipelineFiltersThenProjectsUsingInstalledProviders() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-project-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[\"row-12\",12]", "[\"row-14\",14]");
  }

  @Test
  void invalidSqlProjectionFailsBeforeReadingSource() throws Exception {
    Result result =
        run(
            "source.plugin=fake\nsink.plugin=console\n"
                + "transforms=[{plugin=sql-project,options={columns=\"missing AS label\"}}]");
    assertThat(result.code()).isEqualTo(1);
    assertThat(result.output()).isEmpty();
    assertThat(result.errors()).contains("Unknown SQL projection field");
  }

  @Test
  void packagedPipelineComputesStringThenProjects() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-string-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[\"ROW-10\",10]", "[\"ROW-11\",11]");
  }

  @Test
  void packagedPipelineComputesLongThenProjects() throws Exception {
    Result result = run(Files.readString(Path.of("..", "examples", "fake-sql-long-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList()).containsExactly("[10,100]", "[11,121]");
  }

  @Test
  void packagedPipelineComputesWithIntegerLiterals() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-long-literals-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList()).containsExactly("[10,99]", "[11,109]");
  }

  @Test
  void packagedPipelineComputesWithStringLiterals() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-string-literals-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[10,\"USER-ROW-10\"]", "[11,\"USER-ROW-11\"]");
  }

  @Test
  void packagedPipelineFiltersWithInAndBetween() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-sql-predicates-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[12,\"row-12\"]", "[14,\"row-14\"]");
  }

  @Test
  void packagedPipelineFiltersWithLikeAndEscape() throws Exception {
    Result result = run(Files.readString(Path.of("..", "examples", "fake-sql-like-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[10,\"row-10\"]", "[12,\"row-12\"]");
  }

  @Test
  void packagedPipelineExtractsNativeInsertKind() throws Exception {
    Result result = run(Files.readString(Path.of("..", "examples", "fake-row-kind-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList()).containsExactly("[10,\"+I\"]", "[11,\"+I\"]");
  }

  @Test
  void packagedPipelineFiltersExactTokens() throws Exception {
    Result result =
        run(Files.readString(Path.of("..", "examples", "fake-filter-token-console.conf")));
    assertThat(result.code()).as(result.errors()).isZero();
    assertThat(result.output().lines().toList())
        .containsExactly("[10,\"row-10\"]", "[12,\"row-12\"]");
  }

  @Test
  void malformedJsonDataFailsInsteadOfBecomingMissingValue() throws Exception {
    Result result =
        run(
            "source { plugin=fake, options.rows=1 }\n"
                + "sink.plugin=console\n"
                + "transforms=[{plugin=json-path,options={field=name,path=\"$.missing\",target=extracted}}]");
    assertThat(result.code()).isEqualTo(1);
    assertThat(result.output()).isEmpty();
    assertThat(result.errors()).contains("Invalid JSON input");
  }
}
