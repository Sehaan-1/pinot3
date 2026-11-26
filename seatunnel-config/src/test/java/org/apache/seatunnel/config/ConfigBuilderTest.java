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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ConfigBuilderTest {

  @Test
  void buildFromInlineString() {
    Config config = new ConfigBuilder().withString("a = 1\nb = hello").build();
    Assertions.assertThat(config.getInt("a")).isEqualTo(1);
    Assertions.assertThat(config.getString("b")).isEqualTo("hello");
  }

  @Test
  void buildFromFile(@TempDir Path tempDir) throws Exception {
    Path file = tempDir.resolve("builder.conf");
    Files.writeString(file, "key = from-file\n");
    Config config = new ConfigBuilder().withFile(file.toFile()).build();
    Assertions.assertThat(config.getString("key")).isEqualTo("from-file");
  }

  @Test
  void buildFromResource() {
    Config config = new ConfigBuilder().withResource("test.conf").build();
    Assertions.assertThat(config.getString("test.key")).isEqualTo("from-resource");
  }

  @Test
  void overridesTakePrecedence() {
    Config config =
        new ConfigBuilder()
            .withString("a = 1\nb = original")
            .withOverrides(Map.of("b", "overridden", "c", "added"))
            .build();
    Assertions.assertThat(config.getInt("a")).isEqualTo(1);
    Assertions.assertThat(config.getString("b")).isEqualTo("overridden");
    Assertions.assertThat(config.getString("c")).isEqualTo("added");
  }

  @Test
  void emptyBuilderProducesEmptyConfig() {
    Config config = new ConfigBuilder().build();
    Assertions.assertThat(config.hasPath("anything")).isFalse();
  }
}
