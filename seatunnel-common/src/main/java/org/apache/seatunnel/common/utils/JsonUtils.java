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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.common.exception.JsonException;

/**
 * Thread-safe JSON helpers backed by a shared Jackson {@link ObjectMapper}. The mapper is
 * configured once and is safe for concurrent use after configuration.
 */
public final class JsonUtils {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private JsonUtils() {}

  /** Serializes the given value to a compact JSON string. */
  public static String toJsonString(Object value) {
    try {
      return OBJECT_MAPPER.writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new JsonException("Failed to serialize value to JSON", e);
    }
  }

  /** Serializes the given value to a pretty-printed JSON string. */
  public static String toPrettyJsonString(Object value) {
    try {
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(value);
    } catch (JsonProcessingException e) {
      throw new JsonException("Failed to serialize value to JSON", e);
    }
  }

  /** Deserializes the given JSON string into an instance of the given type. */
  public static <T> T fromJsonString(String json, Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(json, clazz);
    } catch (JsonProcessingException e) {
      throw new JsonException("Failed to deserialize JSON to " + clazz.getName(), e);
    }
  }

  /** Deserializes the given JSON string into the type described by the given reference. */
  public static <T> T fromJsonString(String json, TypeReference<T> typeReference) {
    try {
      return OBJECT_MAPPER.readValue(json, typeReference);
    } catch (JsonProcessingException e) {
      throw new JsonException("Failed to deserialize JSON", e);
    }
  }
}
