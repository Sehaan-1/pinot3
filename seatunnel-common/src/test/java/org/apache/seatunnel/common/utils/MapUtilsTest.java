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

import java.util.HashMap;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class MapUtilsTest {

  private Map<String, Object> sampleMap() {
    Map<String, Object> map = new HashMap<>();
    map.put("string", "hello");
    map.put("int", 42);
    map.put("long", 7L);
    map.put("boolean", true);
    map.put("numericString", "123");
    map.put("nullValue", null);
    return map;
  }

  @Test
  void getStringReturnsValueAndDefault() {
    Map<String, Object> map = sampleMap();
    Assertions.assertThat(MapUtils.getString(map, "string", "fallback")).isEqualTo("hello");
    Assertions.assertThat(MapUtils.getString(map, "missing", "fallback")).isEqualTo("fallback");
    Assertions.assertThat(MapUtils.getString(map, "nullValue", "fallback")).isEqualTo("fallback");
  }

  @Test
  void getIntHandlesNumbersAndNumericStrings() {
    Map<String, Object> map = sampleMap();
    Assertions.assertThat(MapUtils.getInt(map, "int", -1)).isEqualTo(42);
    Assertions.assertThat(MapUtils.getInt(map, "numericString", -1)).isEqualTo(123);
    Assertions.assertThat(MapUtils.getInt(map, "missing", -1)).isEqualTo(-1);
    Assertions.assertThat(MapUtils.getInt(map, "nullValue", -1)).isEqualTo(-1);
  }

  @Test
  void getLongHandlesNumbers() {
    Map<String, Object> map = sampleMap();
    Assertions.assertThat(MapUtils.getLong(map, "long", -1L)).isEqualTo(7L);
    Assertions.assertThat(MapUtils.getLong(map, "int", -1L)).isEqualTo(42L);
    Assertions.assertThat(MapUtils.getLong(map, "missing", -1L)).isEqualTo(-1L);
  }

  @Test
  void getBooleanHandlesBooleans() {
    Map<String, Object> map = sampleMap();
    Assertions.assertThat(MapUtils.getBoolean(map, "boolean", false)).isTrue();
    Assertions.assertThat(MapUtils.getBoolean(map, "missing", false)).isFalse();
    Assertions.assertThat(MapUtils.getBoolean(map, "nullValue", true)).isTrue();
  }
}
