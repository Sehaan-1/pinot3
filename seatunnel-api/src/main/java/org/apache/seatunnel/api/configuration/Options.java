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
package org.apache.seatunnel.api.configuration;

import java.util.Map;
import org.apache.seatunnel.common.exception.ConfigException;

/** Immutable typed option values. Parsing and coercion belong to the configuration layer. */
public final class Options {
  private final Map<String, ?> values;

  public Options(Map<String, ?> values) {
    this.values = Map.copyOf(values);
  }

  public <T> T get(Option<T> option) {
    Object value = values.get(option.key());
    if (value == null) {
      return option
          .defaultValue()
          .orElseThrow(() -> new ConfigException("Missing required option: " + option.key()));
    }
    if (!option.type().isInstance(value)) {
      throw new ConfigException("Invalid type for option: " + option.key());
    }
    return option.type().cast(value);
  }
}
