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
    Path folder = Files.createDirectory(temporary.resolve("run-" + sequence++));
    List<String> args =
        new ArrayList<>(
            List.of(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-jar",
                Path.of("target", "seatunnel-cli-3.0.0-SNAPSHOT.jar").toAbsolutePath().toString()));
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
  void packagedJarRunsTheRealPipelineWithExactDeterministicRows() throws Exception {
    Result result =
        run(
            "env.parallelism=1\nsource { plugin=fake, options { rows=5, start=10, split-size=2 } }\n"
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
}
