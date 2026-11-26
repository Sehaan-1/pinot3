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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import org.apache.seatunnel.engine.plan.TaskId;
import org.apache.seatunnel.engine.task.TaskKey;
import org.apache.seatunnel.engine.task.TaskOutcome;
import org.junit.jupiter.api.Test;

class JobExecutionTest {
  @Test
  void observesSuccessAndReturnsOrderedImmutableOutcomes() throws Exception {
    UUID job = UUID.randomUUID();
    TaskId id = new TaskId("source", 0);
    TaskOutcome result = new TaskOutcome(new TaskKey(job, id), UUID.randomUUID());
    try (var execution = new JobExecution(job, Map.of(id, CompletableFuture.completedFuture(result)))) {
      assertThat(execution.status()).isEqualTo(JobExecution.Status.SUCCEEDED);
      assertThat(execution.await(Duration.ofSeconds(1))).containsExactly(result);
      assertThat(execution.cancel()).isFalse();
      assertThatThrownBy(() -> execution.await(Duration.ofSeconds(1)).clear())
          .isInstanceOf(UnsupportedOperationException.class);
    }
  }

  @Test
  void propagatesFailureAndCancelsPendingSiblingsEvenWhenListedFirst() {
    CompletableFuture<TaskOutcome> pending = new CompletableFuture<>();
    CompletableFuture<TaskOutcome> failed = new CompletableFuture<>();
    failed.completeExceptionally(new IllegalStateException("intentional"));
    Map<TaskId, Future<TaskOutcome>> futures = new LinkedHashMap<>();
    futures.put(new TaskId("a", 0), pending);
    futures.put(new TaskId("b", 0), failed);
    try (var job = new JobExecution(UUID.randomUUID(), futures)) {
      assertThatThrownBy(() -> job.await(Duration.ofSeconds(1)))
          .isInstanceOf(ExecutionException.class).hasCauseInstanceOf(IllegalStateException.class);
      assertThat(job.status()).isEqualTo(JobExecution.Status.FAILED);
      assertThat(pending.isCancelled()).isTrue();
    }
  }

  @Test
  void timeoutAndCloseCancelOutstandingWork() {
    CompletableFuture<TaskOutcome> pending = new CompletableFuture<>();
    try (var job = new JobExecution(UUID.randomUUID(), Map.of(new TaskId("s", 0), pending))) {
      assertThatThrownBy(() -> job.await(Duration.ofMillis(50))).isInstanceOf(TimeoutException.class);
      assertThat(job.status()).isEqualTo(JobExecution.Status.TIMED_OUT);
      assertThat(pending.isCancelled()).isTrue();
    }
    CompletableFuture<TaskOutcome> another = new CompletableFuture<>();
    var job = new JobExecution(UUID.randomUUID(), Map.of(new TaskId("s", 0), another));
    job.close();
    job.close();
    assertThat(another.isCancelled()).isTrue();
    assertThat(job.status()).isEqualTo(JobExecution.Status.CANCELLED);
    assertThatThrownBy(() -> job.await(Duration.ofSeconds(1))).isInstanceOf(CancellationException.class);
  }

  @Test
  void interruptionCancelsWorkAndPreservesInterruptFlag() {
    CompletableFuture<TaskOutcome> pending = new CompletableFuture<>();
    try (var job = new JobExecution(UUID.randomUUID(), Map.of(new TaskId("s", 0), pending))) {
      Thread.currentThread().interrupt();
      try {
        assertThatThrownBy(() -> job.await(Duration.ofSeconds(1))).isInstanceOf(InterruptedException.class);
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
        assertThat(pending.isCancelled()).isTrue();
      } finally { Thread.interrupted(); }
    }
  }

  @Test
  void validatesTimeoutAndMemberSettings() {
    try (var job = new JobExecution(UUID.randomUUID(), Map.of(new TaskId("s", 0), new CompletableFuture<>()))) {
      assertThatThrownBy(() -> job.await(Duration.ZERO)).isInstanceOf(IllegalArgumentException.class);
    }
    assertThatThrownBy(() -> new EngineSettings("", "127.0.0.1", 0, java.util.List.of(), 1, 1, 1, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> new EngineSettings("test", "127.0.0.1", -1, java.util.List.of(), 1, 1, 1, Duration.ofSeconds(1)))
        .isInstanceOf(IllegalArgumentException.class);
  }
}
