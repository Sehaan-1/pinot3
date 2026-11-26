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

import java.util.Objects;
import java.util.Optional;

/** Typed option metadata. Null values are not valid configured values. */
public final class Option<T> {
  private final String key;
  private final Class<T> type;
  private final T defaultValue;

  private Option(String key, Class<T> type, T defaultValue) {
    if (Objects.requireNonNull(key, "key").isBlank()) {
      throw new IllegalArgumentException("Option key must not be blank");
    }
    this.key = key;
    this.type = Objects.requireNonNull(type, "type");
    this.defaultValue = defaultValue == null ? null : type.cast(defaultValue);
  }

  public static <T> Option<T> required(String key, Class<T> type) {
    return new Option<>(key, type, null);
  }

  public static <T> Option<T> withDefault(String key, Class<T> type, T value) {
    return new Option<>(key, type, Objects.requireNonNull(value, "value"));
  }

  public String key() {
    return key;
  }

  public Class<T> type() {
    return type;
  }

  public Optional<T> defaultValue() {
    return Optional.ofNullable(defaultValue);
  }
}
