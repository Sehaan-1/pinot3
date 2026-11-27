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

import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.seatunnel.config.Config;
import org.apache.seatunnel.core.JobConfig;
import org.apache.seatunnel.core.LocalRunner;
import org.apache.seatunnel.plugin.PluginDiscovery;
import org.apache.seatunnel.plugin.SourcePlugin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SeaTunnelCommandTest {
  @TempDir Path temporary;

  @Test
  void discoversFactoriesAndRunsRealOperatorsWithoutClosingTheClasspath() throws Exception {
    ClassLoader previous = Thread.currentThread().getContextClassLoader();
    try (var discovery = PluginDiscovery.fromClasspath(SourcePlugin.class)) {
      assertThat(discovery.identifiers()).containsExactlyInAnyOrder("fake", "jdbc-postgres");
    }
    StringWriter output = new StringWriter();
    long count =
        new LocalRunner()
            .run(
                JobConfig.parse(
                    Config.parseString(
                        "source { plugin=fake, options { rows=3, start=10, split-size=1 } }\n"
                            + "sink.plugin=console")),
                output);
    assertThat(count).isEqualTo(3);
    assertThat(output.toString()).isEqualTo("[10,\"row-10\"]\n[11,\"row-11\"]\n[12,\"row-12\"]\n");
    assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(previous);
  }

  @Test
  void exposesStableCliExitCodesAndSeparateDataAndDiagnostics() throws Exception {
    StringWriter output = new StringWriter();
    StringWriter errors = new StringWriter();
    assertThat(SeaTunnelCommand.execute(new String[] {"--help"}, output, errors)).isZero();
    assertThat(output.toString()).contains("--config");
    assertThat(errors.toString()).isEmpty();
    Path config = temporary.resolve("job.conf");
    Files.writeString(config, "source.plugin=fake\nsink.plugin=console");
    assertThat(
            SeaTunnelCommand.execute(
                new String[] {"--config", config.toString(), "--mode", "cluster"},
                new StringWriter(),
                errors))
        .isEqualTo(2);
    assertThat(
            SeaTunnelCommand.execute(
                new String[] {"--config", temporary.resolve("absent.conf").toString()},
                new StringWriter(),
                errors))
        .isEqualTo(1);
  }
}
