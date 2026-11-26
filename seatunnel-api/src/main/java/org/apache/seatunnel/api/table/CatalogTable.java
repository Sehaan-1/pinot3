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
package org.apache.seatunnel.api.table;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/** Immutable named schema with ordered columns and unique column names. */
public record CatalogTable(String name, List<Column> columns) implements Serializable {
  public CatalogTable {
    if (Objects.requireNonNull(name, "name").isBlank()) {
      throw new IllegalArgumentException("Table name must not be blank");
    }
    columns = List.copyOf(columns);
    if (columns.isEmpty()) {
      throw new IllegalArgumentException("A table must contain at least one column");
    }
    var names = new HashSet<String>();
    for (Column column : columns) {
      if (!names.add(column.name())) {
        throw new IllegalArgumentException("Duplicate column: " + column.name());
      }
    }
  }

  public enum DataType {
    BOOLEAN,
    INT,
    LONG,
    FLOAT,
    DOUBLE,
    STRING,
    BYTES,
    DECIMAL,
    DATE,
    TIME,
    TIMESTAMP
  }

  public record Column(String name, DataType type, boolean nullable) implements Serializable {
    public Column {
      if (Objects.requireNonNull(name, "name").isBlank()) {
        throw new IllegalArgumentException("Column name must not be blank");
      }
      Objects.requireNonNull(type, "type");
    }
  }
}
