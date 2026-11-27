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
package org.apache.seatunnel.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.seatunnel.api.table.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

/** Runtime-specific ordered ownership and row propagation, bounded to 32 stack frames. */
final class TransformChain implements AutoCloseable {
  @FunctionalInterface
  interface Output {
    void emit(SeaTunnelRow row) throws Exception;
  }

  private final List<SeaTunnelTransform> operators = new ArrayList<>();
  private final List<Integer> arities = new ArrayList<>();

  void add(SeaTunnelTransform transform) {
    Objects.requireNonNull(transform, "transform");
    if (operators.size() == 32) throw new IllegalArgumentException("At most 32 transforms");
    operators.add(transform);
    arities.add(
        Objects.requireNonNull(transform.producedTable(), "producedTable").columns().size());
  }

  void open() throws Exception {
    for (var transform : operators) transform.open();
  }

  void emit(SeaTunnelRow row, Output output) throws Exception {
    emit(0, row, output);
  }

  private void emit(int index, SeaTunnelRow row, Output output) throws Exception {
    if (Thread.currentThread().isInterrupted())
      throw new InterruptedException("Local job interrupted");
    Objects.requireNonNull(row, "transform row");
    if (index == operators.size()) {
      output.emit(row);
      return;
    }
    List<SeaTunnelRow> rows =
        Objects.requireNonNull(operators.get(index).apply(row), "transform result");
    for (var next : rows) {
      if (Objects.requireNonNull(next, "transform row").getArity() != arities.get(index))
        throw new IllegalArgumentException("Transform output arity mismatch at stage " + index);
      emit(index + 1, next, output);
    }
  }

  @Override
  public void close() throws Exception {
    Exception failure = null;
    for (int i = operators.size() - 1; i >= 0; i--) {
      try {
        operators.get(i).close();
      } catch (Exception error) {
        if (failure == null) failure = error;
        else if (error != failure) failure.addSuppressed(error);
      }
    }
    operators.clear();
    arities.clear();
    if (failure != null) throw failure;
  }
}
