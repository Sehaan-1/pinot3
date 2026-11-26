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
package org.apache.seatunnel.common.utils;

import java.util.Map;

/** Typed accessors for string-keyed maps with fallback defaults. */
public final class MapUtils {

  private MapUtils() {}

  /** Returns the value for the key as a string, or the default if absent or null. */
  public static String getString(Map<String, Object> map, String key, String defaultValue) {
    Object value = map.get(key);
    return value == null ? defaultValue : String.valueOf(value);
  }

  /** Returns the value for the key as an int, or the default if absent or null. */
  public static int getInt(Map<String, Object> map, String key, int defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number) {
      return ((Number) value).intValue();
    }
    return Integer.parseInt(String.valueOf(value));
  }

  /** Returns the value for the key as a long, or the default if absent or null. */
  public static long getLong(Map<String, Object> map, String key, long defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Number) {
      return ((Number) value).longValue();
    }
    return Long.parseLong(String.valueOf(value));
  }

  /** Returns the value for the key as a boolean, or the default if absent or null. */
  public static boolean getBoolean(Map<String, Object> map, String key, boolean defaultValue) {
    Object value = map.get(key);
    if (value == null) {
      return defaultValue;
    }
    if (value instanceof Boolean) {
      return (Boolean) value;
    }
    return Boolean.parseBoolean(String.valueOf(value));
  }
}
