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

import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;
import org.apache.seatunnel.common.exception.JsonException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonUtilsTest {

  @Test
  void toJsonStringSerializesMap() {
    String json = JsonUtils.toJsonString(Map.of("name", "seatunnel", "count", 3));
    Assertions.assertThat(json).contains("\"name\":\"seatunnel\"");
    Assertions.assertThat(json).contains("\"count\":3");
  }

  @Test
  void toPrettyJsonStringIsReadable() {
    String json = JsonUtils.toPrettyJsonString(Map.of("name", "seatunnel"));
    Assertions.assertThat(json).contains("\"name\" : \"seatunnel\"");
  }

  @Test
  void fromJsonStringDeserializesToClass() {
    Map<?, ?> map = JsonUtils.fromJsonString("{\"name\":\"seatunnel\"}", Map.class);
    Assertions.assertThat(map).containsEntry("name", "seatunnel");
  }

  @Test
  void fromJsonStringDeserializesToTypeReference() {
    List<String> list =
        JsonUtils.fromJsonString("[\"a\",\"b\"]", new TypeReference<List<String>>() {});
    Assertions.assertThat(list).containsExactly("a", "b");
  }

  @Test
  void roundTripPreservesData() {
    Map<String, Object> original = Map.of("name", "seatunnel", "count", 3);
    String json = JsonUtils.toJsonString(original);
    Map<String, Object> restored =
        JsonUtils.fromJsonString(json, new TypeReference<Map<String, Object>>() {});
    Assertions.assertThat(restored).containsEntry("name", "seatunnel");
    Assertions.assertThat(restored).containsEntry("count", 3);
  }

  @Test
  void invalidJsonThrowsJsonException() {
    Assertions.assertThatThrownBy(() -> JsonUtils.fromJsonString("{invalid", Map.class))
        .isInstanceOf(JsonException.class);
  }

  @Test
  void unserializableValueThrowsJsonException() {
    Object bean =
        new Object() {
          public String getBoom() {
            throw new IllegalStateException("boom");
          }
        };
    Assertions.assertThatThrownBy(() -> JsonUtils.toJsonString(bean))
        .isInstanceOf(JsonException.class);
  }
}
