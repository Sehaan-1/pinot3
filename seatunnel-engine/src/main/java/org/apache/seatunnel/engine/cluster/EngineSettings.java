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
package org.apache.seatunnel.engine.cluster;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Explicit TCP member settings. Port zero requests an ephemeral port, useful for embedded callers.
 */
public record EngineSettings(
    String clusterName,
    String bindAddress,
    int port,
    List<String> seeds,
    int workerThreads,
    int queueCapacity,
    int maxJobs,
    Duration shutdownTimeout) {
  public EngineSettings {
    if (Objects.requireNonNull(clusterName, "clusterName").isBlank()
        || Objects.requireNonNull(bindAddress, "bindAddress").isBlank()) {
      throw new IllegalArgumentException("Cluster name and bind address must not be blank");
    }
    seeds = List.copyOf(seeds);
    if (seeds.stream().anyMatch(String::isBlank)
        || port < 0
        || port > 65535
        || workerThreads <= 0
        || queueCapacity <= 0
        || maxJobs <= 0) {
      throw new IllegalArgumentException("Invalid member port, seeds or execution bounds");
    }
    if (shutdownTimeout.isNegative() || shutdownTimeout.isZero()) {
      throw new IllegalArgumentException("Shutdown timeout must be positive");
    }
    shutdownTimeout.toNanos();
  }
}
