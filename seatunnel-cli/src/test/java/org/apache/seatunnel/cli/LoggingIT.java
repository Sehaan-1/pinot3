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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/** Verifies the executable JAR's default logging configuration, not a test resource. */
class LoggingIT {
  @TempDir Path temporary;

  @Test
  void packagedLoggingKeepsWarningsOffTheJsonDataChannel() throws Exception {
    // Do not put target/test-classes first: the packaged CLI owns its logging defaults.
    String classpath =
        Path.of("target", "seatunnel-cli-3.0.0-SNAPSHOT.jar").toAbsolutePath()
            + File.pathSeparator
            + Path.of("target", "test-classes").toAbsolutePath();
    Path stdout = temporary.resolve("stdout.txt");
    Path stderr = temporary.resolve("stderr.txt");
    Process process =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp",
                classpath,
                Probe.class.getName())
            .redirectOutput(stdout.toFile())
            .redirectError(stderr.toFile())
            .start();
    try {
      assertThat(process.waitFor(30, TimeUnit.SECONDS)).as("logging subprocess completed").isTrue();
      assertThat(process.exitValue()).as(Files.readString(stderr)).isZero();
      assertThat(Files.readString(stdout)).isEqualTo("[1]\n");
      assertThat(Files.readString(stderr)).contains("CLI_LOGGING_REGRESSION_PROBE");
      assertThat(Files.readString(stderr)).doesNotContain("JDBC_PRIVATE_DRIVER_DETAILS");
    } finally {
      if (process.isAlive()) process.destroyForcibly().waitFor();
    }
  }

  /** Test-only subprocess entry point; SLF4J/Logback come from the packaged executable JAR. */
  public static final class Probe {
    public static void main(String[] args) {
      LoggerFactory.getLogger(Probe.class).warn("CLI_LOGGING_REGRESSION_PROBE");
      LoggerFactory.getLogger("com.zaxxer.hikari.pool.HikariPool")
          .warn("JDBC_PRIVATE_DRIVER_DETAILS");
      System.out.print("[1]\n");
    }
  }
}
