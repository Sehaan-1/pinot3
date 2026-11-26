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
package org.apache.seatunnel.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.seatunnel.common.exception.ConfigException;
import org.apache.seatunnel.config.Config;
import org.junit.jupiter.api.Test;

class JobConfigTest {
  @Test
  void resolvesHoconAndPreservesTypedOptions() {
    JobConfig config = JobConfig.parse(Config.parseString(
        "env.parallelism=1\nsource { plugin=fake, options { rows=3, start=${env.parallelism} } }\n"
            + "sink.plugin=console"));
    assertThat(config.sourceId()).isEqualTo("fake");
    assertThat(config.sourceOptions()).containsEntry("rows", 3).containsEntry("start", 1);
    assertThat(config.sinkOptions()).isEmpty();
  }

  @Test
  void rejectsUnknownKeysMissingSectionsUnsupportedParallelismAndUnresolvedValues() {
    for (String text : new String[] {
        "source.plugin=fake", "source.plugin=fake\nsink.plugin=console\ntypo=true",
        "source.plugin=fake\nsink.plugin=console\nenv.parallelism=2",
        "source.plugin=fake\nsink.plugin=console\nsource.unknown=1",
        "source.plugin=${absent}\nsink.plugin=console", "source.plugin=BAD\nsink.plugin=console"}) {
      assertThatThrownBy(() -> JobConfig.parse(Config.parseString(text)))
          .isInstanceOf(ConfigException.class);
    }
  }

  @Test
  void copiesTheOptionMaps() {
    Map<String, Object> options = new HashMap<>(Map.of("rows", 2));
    JobConfig config = new JobConfig("fake", options, "console", Map.of());
    options.clear();
    assertThat(config.sourceOptions()).containsEntry("rows", 2);
    assertThatThrownBy(() -> config.sourceOptions().clear())
        .isInstanceOf(UnsupportedOperationException.class);
  }
}
