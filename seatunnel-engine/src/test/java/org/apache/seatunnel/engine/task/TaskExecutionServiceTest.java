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
package org.apache.seatunnel.engine.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.seatunnel.engine.plan.TaskId;
import org.junit.jupiter.api.Test;

class TaskExecutionServiceTest {
  /** Adapts Micrometer's close method to Java resource ownership without replacing its behavior. */
  private static final class OwnedRegistry extends SimpleMeterRegistry implements AutoCloseable {}

  private TaskKey key(String name) {
    return new TaskKey(UUID.randomUUID(), new TaskId(name, 0));
  }

  @Test
  void runsRealTasksAndRecordsSuccessFailureAndDurationWithoutClosingCallerRegistry()
      throws Exception {
    UUID worker = UUID.randomUUID();
    try (OwnedRegistry registry = new OwnedRegistry()) {
      try (var service =
          new TaskExecutionService(worker, null, 1, 2, Duration.ofSeconds(5), registry)) {
        TaskKey key = key("success");
        assertThat(
                service
                    .submit(key, context -> assertThat(context.workerId()).isEqualTo(worker))
                    .get(5, TimeUnit.SECONDS))
            .isEqualTo(new TaskOutcome(key, worker));
        var failed =
            service.submit(
                key("failure"),
                context -> {
                  throw new IllegalStateException("intentional");
                });
        assertThatThrownBy(() -> failed.get(5, TimeUnit.SECONDS))
            .isInstanceOf(ExecutionException.class)
            .hasCauseInstanceOf(IllegalStateException.class);
        await()
            .atMost(Duration.ofSeconds(5))
            .untilAsserted(
                () -> {
                  assertThat(registry.get("seatunnel.task.succeeded").counter().count())
                      .isEqualTo(1);
                  assertThat(registry.get("seatunnel.task.failed").counter().count()).isEqualTo(1);
                });
        assertThat(registry.get("seatunnel.task.submitted").counter().count()).isEqualTo(2);
        assertThat(registry.get("seatunnel.task.duration").timer().count()).isEqualTo(2);
      }
      assertThat(registry.isClosed()).isFalse();
      assertThat(registry.getMeters()).isEmpty();
    }
  }

  @Test
  void enforcesQueueBoundsRejectsDuplicateActiveKeysAndCancelsQueuedAndRunningWork()
      throws Exception {
    try (OwnedRegistry registry = new OwnedRegistry();
        var service =
            new TaskExecutionService(
                UUID.randomUUID(), null, 1, 1, Duration.ofSeconds(5), registry)) {
      CountDownLatch started = new CountDownLatch(1);
      CountDownLatch stopped = new CountDownLatch(1);
      TaskKey active = key("running");
      var running =
          service.submit(
              active,
              context -> {
                started.countDown();
                try {
                  new CountDownLatch(1).await();
                } finally {
                  stopped.countDown();
                }
              });
      assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
      assertThatThrownBy(() -> service.submit(active, context -> {}))
          .isInstanceOf(RejectedExecutionException.class)
          .hasMessageContaining("already active");
      TaskKey queued = key("queued");
      var pending =
          service.submit(
              queued,
              context -> {
                throw new AssertionError("Cancelled work must not execute");
              });
      assertThatThrownBy(() -> service.submit(key("overflow"), context -> {}))
          .isInstanceOf(RejectedExecutionException.class);
      assertThat(service.cancel(queued)).isTrue();
      assertThat(pending.isCancelled()).isTrue();
      assertThat(service.cancel(active)).isTrue();
      assertThat(stopped.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(running.isCancelled()).isTrue();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThat(registry.get("seatunnel.task.cancelled").counter().count())
                      .isEqualTo(2));
      assertThat(registry.get("seatunnel.task.rejected").counter().count()).isEqualTo(2);
    }
  }

  @Test
  void shutdownInterruptsAndRejectsFurtherSubmissionAndIsIdempotent() throws Exception {
    try (OwnedRegistry registry = new OwnedRegistry()) {
      var service =
          new TaskExecutionService(UUID.randomUUID(), null, 1, 1, Duration.ofSeconds(5), registry);
      CountDownLatch started = new CountDownLatch(1);
      try {
        var running =
            service.submit(
                key("blocked"),
                context -> {
                  started.countDown();
                  new CountDownLatch(1).await();
                });
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
        service.close();
        service.close();
        assertThat(running.isCancelled()).isTrue();
        assertThatThrownBy(() -> service.submit(key("late"), context -> {}))
            .isInstanceOf(RejectedExecutionException.class)
            .hasMessageContaining("closed");
      } finally {
        service.close();
      }
    }
  }

  @Test
  void validatesBoundsBeforeAllocatingExecutorResources() {
    try (OwnedRegistry registry = new OwnedRegistry()) {
      assertThatThrownBy(
              () ->
                  new TaskExecutionService(
                      UUID.randomUUID(), null, 0, 1, Duration.ofSeconds(1), registry))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  new TaskExecutionService(
                      UUID.randomUUID(), null, 1, 0, Duration.ofSeconds(1), registry))
          .isInstanceOf(IllegalArgumentException.class);
      assertThatThrownBy(
              () ->
                  new TaskExecutionService(UUID.randomUUID(), null, 1, 1, Duration.ZERO, registry))
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(registry.getMeters()).isEmpty();
    }
  }
}
