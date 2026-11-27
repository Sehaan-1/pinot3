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
import java.util.Arrays;
import java.util.Objects;

/** A positional row with immutable change metadata; mutable field values remain caller-owned. */
public final class SeaTunnelRow implements Serializable {
  private static final long serialVersionUID = 1L;
  private final Object[] fields;
  private final RowKind rowKind;

  public SeaTunnelRow(Object... fields) {
    this(RowKind.INSERT, fields);
  }

  private SeaTunnelRow(RowKind kind, Object[] fields) {
    this.rowKind = Objects.requireNonNull(kind, "kind");
    this.fields = Objects.requireNonNull(fields, "fields").clone();
  }

  /** Explicit metadata factory; never interprets the first data field as metadata. */
  public static SeaTunnelRow ofKind(RowKind kind, Object... fields) {
    return new SeaTunnelRow(kind, fields);
  }

  /** Missing kind in a serialVersionUID1 legacy stream means INSERT. */
  public RowKind getRowKind() {
    return rowKind == null ? RowKind.INSERT : rowKind;
  }

  public SeaTunnelRow withKind(RowKind kind) {
    return new SeaTunnelRow(kind, fields);
  }

  public SeaTunnelRow withFields(Object... fields) {
    return new SeaTunnelRow(getRowKind(), fields);
  }

  public int getArity() {
    return fields.length;
  }

  public Object getField(int index) {
    return fields[index];
  }

  public Object[] getFields() {
    return fields.clone();
  }

  @Override
  public boolean equals(Object other) {
    return other instanceof SeaTunnelRow
        && getRowKind() == ((SeaTunnelRow) other).getRowKind()
        && Arrays.deepEquals(fields, ((SeaTunnelRow) other).fields);
  }

  @Override
  public int hashCode() {
    int values = Arrays.deepHashCode(fields);
    return getRowKind() == RowKind.INSERT ? values : 31 * values + getRowKind().hashCode();
  }

  @Override
  public String toString() {
    return Arrays.deepToString(fields);
  }
}
