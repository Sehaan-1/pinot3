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

import com.google.common.base.Splitter;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.seatunnel.api.table.CatalogTable;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Exact literal token membership; Guava owns lazy tokenization. */
public final class FilterTokenTransform implements SeaTunnelTransform {
  public static final int MAX_INPUT_CHARS = 1_048_576;

  public enum MatchMode {
    ANY,
    ALL
  }

  private final CatalogTable input;
  private final int index;
  private final Splitter splitter;
  private final Set<String> requested;
  private final MatchMode mode;
  private boolean opened, closed;

  public FilterTokenTransform(
      CatalogTable input, String field, String delimiter, List<String> tokens, MatchMode mode) {
    this.input = Objects.requireNonNull(input, "input");
    int found = -1;
    for (int i = 0; i < input.columns().size(); i++)
      if (input.columns().get(i).name().equals(field)) {
        found = i;
        break;
      }
    if (found < 0 || input.columns().get(found).type() != CatalogTable.DataType.STRING)
      throw new IllegalArgumentException("Expected existing STRING token field");
    index = found;
    if (delimiter == null || delimiter.isEmpty() || delimiter.length() > 128)
      throw new IllegalArgumentException("Expected literal delimiter of 1..128 characters");
    if (tokens == null || tokens.isEmpty() || tokens.size() > 128)
      throw new IllegalArgumentException("Expected 1..128 requested tokens");
    for (String token : tokens)
      if (token == null || token.isEmpty() || token.length() > 4096)
        throw new IllegalArgumentException("Expected nonempty token of at most 4096 characters");
    requested = Set.copyOf(tokens);
    if (requested.size() != tokens.size())
      throw new IllegalArgumentException("Requested tokens must be distinct");
    if (mode == null) throw new IllegalArgumentException("Expected token match mode");
    this.mode = mode;
    splitter = Splitter.on(delimiter);
  }

  public CatalogTable producedTable() {
    return input;
  }

  public void open() {
    if (opened || closed)
      throw new IllegalStateException("Transform cannot be opened twice or after close");
    opened = true;
  }

  public List<SeaTunnelRow> apply(SeaTunnelRow row) {
    if (!opened || closed) throw new IllegalStateException("Transform is not open");
    if (Objects.requireNonNull(row, "row").getArity() != input.columns().size())
      throw new IllegalArgumentException("Input arity mismatch");
    Object cell = row.getField(index);
    if (cell == null) return List.of();
    if (!(cell instanceof String value))
      throw new IllegalArgumentException("Expected String token field value");
    if (value.length() > MAX_INPUT_CHARS)
      throw new IllegalArgumentException("Token input exceeds character limit");
    Set<String> matched = mode == MatchMode.ALL ? new HashSet<>() : Set.of();
    for (String token : splitter.split(value))
      if (requested.contains(token)) {
        if (mode == MatchMode.ANY) return List.of(row);
        matched.add(token);
        if (matched.size() == requested.size()) return List.of(row);
      }
    return List.of();
  }

  public void close() {
    closed = true;
  }
}
