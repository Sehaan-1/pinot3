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

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import org.apache.seatunnel.common.exception.ConfigException;

/**
 * Builds a {@link Config} from a file, classpath resource, or inline HOCON text, optionally
 * layered with string overrides. Not thread-safe; configure and build on a single thread.
 */
public final class ConfigBuilder {

  private File file;
  private String resource;
  private String text;
  private final Map<String, String> overrides = new HashMap<>();

  /** Loads config from the given HOCON file. */
  public ConfigBuilder withFile(File file) {
    this.file = file;
    return this;
  }

  /** Loads config from the given HOCON classpath resource. */
  public ConfigBuilder withResource(String resource) {
    this.resource = resource;
    return this;
  }

  /** Loads config from the given inline HOCON text. */
  public ConfigBuilder withString(String text) {
    this.text = text;
    return this;
  }

  /** Adds overrides that take precedence over the loaded config. Keys are config paths. */
  public ConfigBuilder withOverrides(Map<String, String> overrides) {
    this.overrides.putAll(overrides);
    return this;
  }

  /** Builds the config, applying overrides on top of the loaded source. */
  public Config build() {
    com.typesafe.config.Config base;
    try {
      if (file != null) {
        base = ConfigFactory.parseFile(file);
      } else if (resource != null) {
        base = ConfigFactory.parseResources(resource);
      } else if (text != null) {
        base = ConfigFactory.parseString(text);
      } else {
        base = ConfigFactory.empty();
      }
      if (!overrides.isEmpty()) {
        base = ConfigFactory.parseMap(overrides).withFallback(base);
      }
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to build config", e);
    }
    return Config.of(base);
  }
}
