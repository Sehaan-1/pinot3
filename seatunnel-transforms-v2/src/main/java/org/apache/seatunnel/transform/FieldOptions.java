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
package org.apache.seatunnel.transform;

import java.util.LinkedHashMap;
import java.util.Map;

/** Strict configuration adaptation; HOCON itself is parsed by Typesafe Config upstream. */
final class FieldOptions {
  private FieldOptions() {}

  static Map<String, String> fields(Map<String, Object> options) {
    if (!options.keySet().equals(java.util.Set.of("fields"))
        || !(options.get("fields") instanceof Map<?, ?> fields))
      throw new IllegalArgumentException("Only a fields object is accepted");
    Map<String, String> result = new LinkedHashMap<>();
    fields.forEach(
        (key, value) -> {
          if (!(key instanceof String source) || !(value instanceof String target))
            throw new IllegalArgumentException("Field mappings must be strings");
          result.put(source, target);
        });
    return result;
  }
}
