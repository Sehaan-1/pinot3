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

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.seatunnel.common.exception.ConfigException;
import org.apache.seatunnel.config.Config;

/** Validated, immutable local job specification. Connector factories validate their own options. */
public record JobConfig(
    String sourceId,
    Map<String, Object> sourceOptions,
    String sinkId,
    Map<String, Object> sinkOptions) {
  public JobConfig {
    validateId(sourceId);
    validateId(sinkId);
    sourceOptions = Map.copyOf(sourceOptions);
    sinkOptions = Map.copyOf(sinkOptions);
  }

  private static void validateId(String id) {
    if (!Objects.requireNonNull(id, "plugin id").matches("[a-z][a-z0-9-]*")) {
      throw new ConfigException("Invalid plugin identifier: " + id);
    }
  }

  public static JobConfig parse(Config input) {
    try {
      Config config = Config.of(input.unwrap().resolve());
      allowed(config, Set.of("env", "source", "sink"));
      config.requireKeys("source", "sink");
      if (config.unwrap().hasPathOrNull("env")) {
        Config env = config.getConfig("env");
        allowed(env, Set.of("parallelism"));
        if (env.getInt("parallelism", 1) != 1) {
          throw new ConfigException("Local baseline supports parallelism = 1 only");
        }
      }
      Config source = config.getConfig("source");
      Config sink = config.getConfig("sink");
      allowed(source, Set.of("plugin", "options"));
      allowed(sink, Set.of("plugin", "options"));
      return new JobConfig(
          source.getString("plugin"), options(source), sink.getString("plugin"), options(sink));
    } catch (com.typesafe.config.ConfigException failure) {
      throw new ConfigException("Invalid or unresolved job configuration", failure);
    }
  }

  private static Map<String, Object> options(Config section) {
    return section.unwrap().hasPathOrNull("options")
        ? section.getConfig("options").unwrap().root().unwrapped()
        : Map.of();
  }

  private static void allowed(Config config, Set<String> keys) {
    for (String key : config.unwrap().root().keySet()) {
      if (!keys.contains(key)) {
        throw new ConfigException("Unknown job configuration key: " + key);
      }
    }
  }
}
