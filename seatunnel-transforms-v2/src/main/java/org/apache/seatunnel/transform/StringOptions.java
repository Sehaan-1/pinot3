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

import java.util.List;
import java.util.Map;
import java.util.Set;

/** Strict option adaptation; Typesafe Config remains the configuration parser. */
final class StringOptions {
  private StringOptions() {}

  static void keys(Map<String, Object> options, Set<String> expected) {
    if (!options.keySet().equals(expected))
      throw new IllegalArgumentException("Unexpected or missing string transform options");
  }

  static String string(Map<String, Object> options, String key) {
    if (!(options.get(key) instanceof String value))
      throw new IllegalArgumentException("Expected string option: " + key);
    return value;
  }

  static List<String> fields(Map<String, Object> options) {
    return strings(options, "fields", "Expected 1..128 string field names");
  }

  static List<String> strings(Map<String, Object> options, String key) {
    return strings(options, key, "Expected 1..128 string values for " + key);
  }

  private static List<String> strings(Map<String, Object> options, String key, String message) {
    if (!(options.get(key) instanceof List<?> values)
        || values.isEmpty()
        || values.size() > 128
        || values.stream().anyMatch(value -> !(value instanceof String)))
      throw new IllegalArgumentException(message);
    return values.stream().map(String.class::cast).toList();
  }
}
