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
package org.apache.seatunnel.config;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.seatunnel.common.exception.ConfigException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigTest {

  private static final String HOCON =
      "app.name = seatunnel\n"
          + "app.port = 8080\n"
          + "app.debug = true\n"
          + "app.ratio = 0.5\n"
          + "app.tags = [\"a\", \"b\"]\n"
          + "server.host = localhost\n"
          + "server.timeout = 30\n";

  @Test
  void typedGettersReadValues() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThat(config.getString("app.name")).isEqualTo("seatunnel");
    Assertions.assertThat(config.getInt("app.port")).isEqualTo(8080);
    Assertions.assertThat(config.getLong("app.port")).isEqualTo(8080L);
    Assertions.assertThat(config.getBoolean("app.debug")).isTrue();
    Assertions.assertThat(config.getDouble("app.ratio")).isEqualTo(0.5);
    Assertions.assertThat(config.getStringList("app.tags")).containsExactly("a", "b");
  }

  @Test
  void nestedConfigIsAccessible() {
    Config config = Config.parseString(HOCON);
    Config server = config.getConfig("server");
    Assertions.assertThat(server.getString("host")).isEqualTo("localhost");
    Assertions.assertThat(server.getInt("timeout")).isEqualTo(30);
  }

  @Test
  void hasPathReflectsPresence() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThat(config.hasPath("app.name")).isTrue();
    Assertions.assertThat(config.hasPath("app.missing")).isFalse();
  }

  @Test
  void defaultsApplyToMissingPaths() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThat(config.getString("app.missing", "fallback")).isEqualTo("fallback");
    Assertions.assertThat(config.getInt("app.missing", -1)).isEqualTo(-1);
    Assertions.assertThat(config.getLong("app.missing", -1L)).isEqualTo(-1L);
    Assertions.assertThat(config.getBoolean("app.missing", true)).isTrue();
    Assertions.assertThat(config.getDouble("app.missing", 1.5)).isEqualTo(1.5);
    Assertions.assertThat(config.getStringList("app.missing", List.of("x")))
        .containsExactly("x");
  }

  @Test
  void missingPathWithoutDefaultThrowsConfigException() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThatThrownBy(() -> config.getString("app.missing"))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void wrongTypeThrowsConfigException() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThatThrownBy(() -> config.getInt("app.name"))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void requireKeysPassesWhenAllPresent() {
    Config config = Config.parseString(HOCON);
    config.requireKeys("app.name", "app.port", "server.host");
  }

  @Test
  void requireKeysThrowsWhenMissing() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThatThrownBy(() -> config.requireKeys("app.name", "app.missing"))
        .isInstanceOf(ConfigException.class)
        .hasMessageContaining("app.missing");
  }

  @Test
  void parseFileReadsHoconFile(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("test.conf");
    Files.writeString(file, HOCON);
    Config config = Config.parseFile(file.toFile());
    Assertions.assertThat(config.getString("app.name")).isEqualTo("seatunnel");
  }

  @Test
  void parseResourceReadsClasspathResource() {
    Config config = Config.parseResource("test.conf");
    Assertions.assertThat(config.getString("test.key")).isEqualTo("from-resource");
    Assertions.assertThat(config.getInt("test.number")).isEqualTo(123);
  }

  @Test
  void unwrapReturnsTypesafeConfig() {
    Config config = Config.parseString(HOCON);
    Assertions.assertThat(config.unwrap()).isNotNull();
    Assertions.assertThat(config.unwrap().getString("app.name")).isEqualTo("seatunnel");
  }

  @Test
  void invalidHoconThrowsConfigException() {
    Assertions.assertThatThrownBy(() -> Config.parseString("app = [unclosed"))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void parseMissingFileThrowsConfigException(@TempDir Path tempDir) {
    File missing = tempDir.resolve("does-not-exist.conf").toFile();
    Assertions.assertThatThrownBy(() -> Config.parseFile(missing))
        .isInstanceOf(ConfigException.class);
  }
}
