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
package org.apache.seatunnel.format;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.io.IOException;
import java.util.Objects;

/** Thread-safe typed JSON adapter; default typing and polymorphic class loading are not enabled. */
public final class JsonFormat<T> implements SerializationSchema<T>, DeserializationSchema<T> {
  private final ObjectReader reader;
  private final ObjectWriter writer;

  public JsonFormat(Class<T> type) {
    this(new ObjectMapper(), Objects.requireNonNull(type, "type"));
  }

  private JsonFormat(ObjectMapper mapper, Class<T> type) {
    reader = mapper.readerFor(type).with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    writer = mapper.writerFor(type);
  }

  public JsonFormat(TypeReference<T> type) {
    ObjectMapper mapper = new ObjectMapper();
    reader =
        mapper
            .readerFor(Objects.requireNonNull(type, "type"))
            .with(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    writer = mapper.writerFor(type);
  }

  @Override
  public byte[] serialize(T value) throws FormatException {
    try {
      return writer.writeValueAsBytes(Objects.requireNonNull(value, "value"));
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot serialize JSON value", e);
    }
  }

  @Override
  public T deserialize(byte[] message) throws FormatException {
    try {
      T value = reader.readValue(Objects.requireNonNull(message, "message"));
      return Objects.requireNonNull(value, "JSON null is not a message value");
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot deserialize JSON message", e);
    }
  }
}
