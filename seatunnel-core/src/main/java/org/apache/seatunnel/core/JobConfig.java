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

import java.util.ArrayList;
import java.util.List;
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
    Map<String, Object> sinkOptions,
    List<TransformConfig> transforms,
    List<SourceConfig> additionalSources) {
  public JobConfig {
    validateId(sourceId);
    validateId(sinkId);
    sourceOptions = Map.copyOf(sourceOptions);
    sinkOptions = Map.copyOf(sinkOptions);
    additionalSources = List.copyOf(additionalSources);
    if (additionalSources.size() > 31) throw new ConfigException("At most 32 sources supported");
    transforms = List.copyOf(transforms);
    if (transforms.size() > 32) throw new ConfigException("At most 32 transforms supported");
  }

  public JobConfig(
      String sourceId,
      Map<String, Object> sourceOptions,
      String sinkId,
      Map<String, Object> sinkOptions) {
    this(sourceId, sourceOptions, sinkId, sinkOptions, List.of());
  }

  public JobConfig(
      String sourceId,
      Map<String, Object> sourceOptions,
      String sinkId,
      Map<String, Object> sinkOptions,
      List<TransformConfig> transforms) {
    this(sourceId, sourceOptions, sinkId, sinkOptions, transforms, List.of());
  }

  public record SourceConfig(String plugin, Map<String, Object> options) {
    public SourceConfig {
      validateId(plugin);
      options = Map.copyOf(options);
    }
  }

  public List<SourceConfig> sources() {
    var result = new ArrayList<SourceConfig>();
    result.add(new SourceConfig(sourceId, sourceOptions));
    result.addAll(additionalSources);
    return List.copyOf(result);
  }

  public record TransformConfig(String plugin, Map<String, Object> options) {
    public TransformConfig {
      validateId(plugin);
      options = Map.copyOf(options);
    }
  }

  private static void validateId(String id) {
    if (!Objects.requireNonNull(id, "plugin id").matches("[a-z][a-z0-9-]*")) {
      throw new ConfigException("Invalid plugin identifier: " + id);
    }
  }

  public static JobConfig parse(Config input) {
    try {
      Config config = Config.of(input.unwrap().resolve());
      allowed(config, Set.of("env", "source", "sources", "sink", "transforms"));
      config.requireKeys("sink");
      boolean single = config.unwrap().hasPathOrNull("source");
      boolean multiple = config.unwrap().hasPathOrNull("sources");
      if (single == multiple) throw new ConfigException("Specify exactly one of source or sources");
      List<SourceConfig> inputs = new ArrayList<>();
      if (single) {
        Config entry = config.getConfig("source");
        allowed(entry, Set.of("plugin", "options"));
        inputs.add(new SourceConfig(entry.getString("plugin"), options(entry)));
      } else {
        var entries = config.unwrap().getConfigList("sources");
        if (entries.size() < 2 || entries.size() > 32)
          throw new ConfigException("sources requires 2..32 inputs");
        for (var raw : entries) {
          Config entry = Config.of(raw);
          allowed(entry, Set.of("plugin", "options"));
          inputs.add(new SourceConfig(entry.getString("plugin"), options(entry)));
        }
      }
      if (config.unwrap().hasPathOrNull("env")) {
        Config env = config.getConfig("env");
        allowed(env, Set.of("parallelism"));
        if (env.getInt("parallelism", 1) != 1) {
          throw new ConfigException("Local baseline supports parallelism = 1 only");
        }
      }
      Config sink = config.getConfig("sink");
      allowed(sink, Set.of("plugin", "options"));
      List<TransformConfig> transforms = new ArrayList<>();
      if (config.unwrap().hasPathOrNull("transforms")) {
        for (var raw : config.unwrap().getConfigList("transforms")) {
          if (transforms.size() == 32) throw new ConfigException("At most 32 transforms supported");
          Config transform = Config.of(raw);
          allowed(transform, Set.of("plugin", "options"));
          transforms.add(new TransformConfig(transform.getString("plugin"), options(transform)));
        }
      }
      return new JobConfig(
          inputs.get(0).plugin(),
          inputs.get(0).options(),
          sink.getString("plugin"),
          options(sink),
          transforms,
          inputs.subList(1, inputs.size()));
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
