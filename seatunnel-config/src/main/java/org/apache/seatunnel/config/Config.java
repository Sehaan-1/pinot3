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
import java.util.List;
import org.apache.seatunnel.common.exception.ConfigException;

/**
 * Thin wrapper around Typesafe Config (HOCON) that exposes typed accessors with defaults and
 * translates Typesafe failures into {@link ConfigException}.
 */
public final class Config {

  private final com.typesafe.config.Config typesafeConfig;

  private Config(com.typesafe.config.Config typesafeConfig) {
    this.typesafeConfig = typesafeConfig;
  }

  /** Wraps an existing Typesafe Config instance. */
  public static Config of(com.typesafe.config.Config typesafeConfig) {
    return new Config(typesafeConfig);
  }

  /** Parses the given HOCON file. */
  public static Config parseFile(File file) {
    try {
      return of(ConfigFactory.parseFile(file));
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to parse config file: " + file, e);
    }
  }

  /** Parses the given HOCON text. */
  public static Config parseString(String text) {
    try {
      return of(ConfigFactory.parseString(text));
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to parse config text", e);
    }
  }

  /** Parses the given HOCON classpath resource. */
  public static Config parseResource(String resource) {
    try {
      return of(ConfigFactory.parseResources(resource));
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to parse config resource: " + resource, e);
    }
  }

  /** Returns true if the given path is present in this config. */
  public boolean hasPath(String path) {
    return typesafeConfig.hasPath(path);
  }

  /** Returns the string value at the given path. */
  public String getString(String path) {
    try {
      return typesafeConfig.getString(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get string config at path: " + path, e);
    }
  }

  /** Returns the string value at the given path, or the default if absent. */
  public String getString(String path, String defaultValue) {
    return typesafeConfig.hasPath(path) ? getString(path) : defaultValue;
  }

  /** Returns the int value at the given path. */
  public int getInt(String path) {
    try {
      return typesafeConfig.getInt(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get int config at path: " + path, e);
    }
  }

  /** Returns the int value at the given path, or the default if absent. */
  public int getInt(String path, int defaultValue) {
    return typesafeConfig.hasPath(path) ? getInt(path) : defaultValue;
  }

  /** Returns the long value at the given path. */
  public long getLong(String path) {
    try {
      return typesafeConfig.getLong(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get long config at path: " + path, e);
    }
  }

  /** Returns the long value at the given path, or the default if absent. */
  public long getLong(String path, long defaultValue) {
    return typesafeConfig.hasPath(path) ? getLong(path) : defaultValue;
  }

  /** Returns the boolean value at the given path. */
  public boolean getBoolean(String path) {
    try {
      return typesafeConfig.getBoolean(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get boolean config at path: " + path, e);
    }
  }

  /** Returns the boolean value at the given path, or the default if absent. */
  public boolean getBoolean(String path, boolean defaultValue) {
    return typesafeConfig.hasPath(path) ? getBoolean(path) : defaultValue;
  }

  /** Returns the double value at the given path. */
  public double getDouble(String path) {
    try {
      return typesafeConfig.getDouble(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get double config at path: " + path, e);
    }
  }

  /** Returns the double value at the given path, or the default if absent. */
  public double getDouble(String path, double defaultValue) {
    return typesafeConfig.hasPath(path) ? getDouble(path) : defaultValue;
  }

  /** Returns the string list value at the given path. */
  public List<String> getStringList(String path) {
    try {
      return typesafeConfig.getStringList(path);
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get string list config at path: " + path, e);
    }
  }

  /** Returns the string list value at the given path, or the default if absent. */
  public List<String> getStringList(String path, List<String> defaultValue) {
    return typesafeConfig.hasPath(path) ? getStringList(path) : defaultValue;
  }

  /** Returns the nested config at the given path. */
  public Config getConfig(String path) {
    try {
      return of(typesafeConfig.getConfig(path));
    } catch (com.typesafe.config.ConfigException e) {
      throw new ConfigException("Failed to get config at path: " + path, e);
    }
  }

  /** Throws a {@link ConfigException} if any of the given paths is missing. */
  public void requireKeys(String... keys) {
    for (String key : keys) {
      if (!typesafeConfig.hasPath(key)) {
        throw new ConfigException("Missing required config at path: " + key);
      }
    }
  }

  /** Returns the underlying Typesafe Config instance. */
  public com.typesafe.config.Config unwrap() {
    return typesafeConfig;
  }
}
