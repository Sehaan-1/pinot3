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
package org.apache.seatunnel.engine.checkpoint;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.seatunnel.api.source.SourceSplit;

/** Caller-confined split ownership. Restoring requeues unfinished work (at-least-once). */
public final class SplitLedger<S extends SourceSplit> {
  public record Snapshot<S extends SourceSplit>(
      List<S> splits, Map<String, Integer> assigned, Set<String> completed)
      implements Serializable {
    public Snapshot {
      splits = List.copyOf(splits);
      assigned = Map.copyOf(assigned);
      completed = Set.copyOf(completed);
    }
  }

  private final int parallelism;
  private final Map<String, S> splits = new LinkedHashMap<>();
  private final Map<String, Integer> assigned = new LinkedHashMap<>();
  private final Set<String> completed = new LinkedHashSet<>();

  public SplitLedger(int parallelism, List<S> initial) {
    if (parallelism <= 0) throw new IllegalArgumentException("positive parallelism required");
    this.parallelism = parallelism;
    for (S split : initial) {
      String id = java.util.Objects.requireNonNull(split.splitId(), "splitId");
      if (id.isBlank() || splits.putIfAbsent(id, split) != null)
        throw new IllegalArgumentException("blank or duplicate split ID: " + id);
    }
  }

  public List<S> assign(int reader, int maximum) {
    reader(reader);
    if (maximum <= 0) throw new IllegalArgumentException("positive assignment bound required");
    List<S> result = new ArrayList<>();
    for (var entry : splits.entrySet()) {
      if (!completed.contains(entry.getKey()) && !assigned.containsKey(entry.getKey())) {
        assigned.put(entry.getKey(), reader);
        result.add(entry.getValue());
        if (result.size() == maximum) break;
      }
    }
    return List.copyOf(result);
  }

  public void complete(int reader, String splitId) {
    reader(reader);
    if (!Integer.valueOf(reader).equals(assigned.get(splitId)))
      throw new IllegalArgumentException("Reader does not own split: " + splitId);
    assigned.remove(splitId);
    completed.add(splitId);
  }

  public void readerFailed(int reader) {
    reader(reader);
    assigned.values().removeIf(owner -> owner == reader);
  }

  public Snapshot<S> snapshot() {
    return new Snapshot<>(new ArrayList<>(splits.values()), assigned, completed);
  }

  public static <S extends SourceSplit> SplitLedger<S> restore(int parallelism, Snapshot<S> state) {
    SplitLedger<S> ledger = new SplitLedger<>(parallelism, state.splits());
    Set<String> known = ledger.splits.keySet();
    if (!known.containsAll(state.completed())
        || !known.containsAll(state.assigned().keySet())
        || !java.util.Collections.disjoint(state.completed(), state.assigned().keySet())
        || state.assigned().values().stream()
            .anyMatch(reader -> reader < 0 || reader >= parallelism))
      throw new IllegalArgumentException("Invalid split snapshot");
    ledger.completed.addAll(state.completed());
    // Deliberately discard assignments: prior readers are no longer trusted after recovery.
    return ledger;
  }

  private void reader(int reader) {
    if (reader < 0 || reader >= parallelism)
      throw new IllegalArgumentException("Unknown reader: " + reader);
  }
}
