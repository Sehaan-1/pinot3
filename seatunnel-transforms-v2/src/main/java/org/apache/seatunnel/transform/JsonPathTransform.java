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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import com.jayway.jsonpath.spi.json.JacksonJsonProvider;
import com.jayway.jsonpath.spi.mapper.JacksonMappingProvider;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Trusted Jayway queries over bounded JSON strings; not a CPU sandbox for query expressions. */
public final class JsonPathTransform implements SeaTunnelTransform {
  public static final int MAX_INPUT_CHARS = 1_048_576;
  private final int arity, index;
  private final CatalogTable output;
  private final JsonPath query;
  private final ObjectMapper mapper;
  private final Configuration configuration;
  private boolean opened, closed;

  public JsonPathTransform(CatalogTable input, String field, String path, String target) {
    Objects.requireNonNull(input, "input");
    if (path == null || path.isBlank() || path.length() > 4096)
      throw new IllegalArgumentException(
          "Expected a nonblank JSON path of at most 4096 characters");
    query = JsonPath.compile(path);
    arity = input.columns().size();
    int found = -1;
    for (int i = 0; i < arity; i++)
      if (input.columns().get(i).name().equals(field)) {
        found = i;
        break;
      }
    if (found < 0) throw new IllegalArgumentException("Unknown source field");
    index = found;
    if (input.columns().get(index).type() != CatalogTable.DataType.STRING)
      throw new IllegalArgumentException("JSON input field must have STRING type");
    var columns = new ArrayList<>(input.columns());
    columns.add(new CatalogTable.Column(target, CatalogTable.DataType.STRING, true));
    output = new CatalogTable(input.name(), columns);
    JsonFactory factory =
        JsonFactory.builder()
            .streamReadConstraints(
                StreamReadConstraints.builder()
                    .maxNestingDepth(64)
                    .maxStringLength(MAX_INPUT_CHARS)
                    .build())
            .build();
    mapper =
        new ObjectMapper(factory)
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    configuration =
        Configuration.builder()
            .jsonProvider(new JacksonJsonProvider(mapper))
            .mappingProvider(new JacksonMappingProvider(mapper))
            .build();
  }

  public CatalogTable producedTable() {
    return output;
  }

  public void open() {
    if (opened || closed)
      throw new IllegalStateException("Transform cannot be opened twice or after close");
    opened = true;
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    if (!opened || closed) throw new IllegalStateException("Transform is not open");
    if (Objects.requireNonNull(row, "row").getArity() != arity)
      throw new IllegalArgumentException("Input arity mismatch");
    Object cell = row.getField(index);
    if (cell != null && !(cell instanceof String))
      throw new IllegalArgumentException("Expected String JSON field value");
    Object[] values = java.util.Arrays.copyOf(row.getFields(), arity + 1);
    if (cell != null) values[arity] = extract((String) cell);
    return List.of(row.withFields(values));
  }

  private String extract(String input) {
    if (input.length() > MAX_INPUT_CHARS)
      throw new IllegalArgumentException("JSON input exceeds character limit");
    // Parse separately: malformed input must never be mistaken for a missing path.
    Object document;
    try {
      document = configuration.jsonProvider().parse(input);
    } catch (RuntimeException invalid) {
      throw new IllegalArgumentException("Invalid JSON input", invalid);
    }
    if (document == null) return null;
    Object selected;
    try {
      selected = query.read(document, configuration);
    } catch (PathNotFoundException missing) {
      return null;
    } catch (RuntimeException failure) {
      throw new IllegalArgumentException("JSON path evaluation failed", failure);
    }
    if (selected == null) return null;
    if (selected instanceof String text) return text;
    try {
      return mapper.writeValueAsString(selected);
    } catch (JsonProcessingException failure) {
      throw new IllegalArgumentException("JSON result serialization failed", failure);
    }
  }

  public void close() {
    closed = true;
  }
}
