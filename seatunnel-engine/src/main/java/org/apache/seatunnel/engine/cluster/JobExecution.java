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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.seatunnel.engine.plan.TaskId;
import org.apache.seatunnel.engine.task.TaskOutcome;

/**
 * Owned Future-like handle. Completion/failure is observed by status or await, not a hidden thread.
 */
public final class JobExecution implements AutoCloseable {
  public enum Status {
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELLED,
    TIMED_OUT
  }

  private final UUID jobId;
  private final Map<TaskId, Future<TaskOutcome>> futures;
  private Status status = Status.RUNNING;
  private Throwable failure;
  private List<TaskOutcome> results = List.of();

  JobExecution(UUID jobId, Map<TaskId, Future<TaskOutcome>> futures) {
    this.jobId = jobId;
    this.futures = java.util.Collections.unmodifiableMap(new LinkedHashMap<>(futures));
  }

  public UUID jobId() {
    return jobId;
  }

  public synchronized Status status() {
    refresh();
    return status;
  }

  private void refresh() {
    if (status != Status.RUNNING) {
      return;
    }
    boolean complete = true;
    List<TaskOutcome> completed = new ArrayList<>();
    for (Future<TaskOutcome> future : futures.values()) {
      if (!future.isDone()) {
        complete = false;
        continue;
      }
      try {
        completed.add(future.get());
      } catch (ExecutionException error) {
        failure = error.getCause();
        status = Status.FAILED;
        cancelFutures(failure);
        return;
      } catch (CancellationException error) {
        failure = error;
        status = Status.CANCELLED;
        cancelFutures(error);
        return;
      } catch (InterruptedException error) {
        failure = error;
        status = Status.CANCELLED;
        cancelFutures(error);
        Thread.currentThread().interrupt();
        return;
      }
    }
    if (complete) {
      results = List.copyOf(completed);
      status = Status.SUCCEEDED;
    }
  }

  private void cancelFutures(Throwable primary) {
    for (Future<TaskOutcome> future : futures.values()) {
      try {
        if (!future.isDone()) {
          future.cancel(true);
        }
      } catch (RuntimeException cancellationFailure) {
        primary.addSuppressed(cancellationFailure);
      }
    }
  }

  /** One overall deadline; timeout/interruption cancels outstanding tasks best-effort. */
  public List<TaskOutcome> await(Duration timeout)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeout.isZero() || timeout.isNegative()) {
      throw new IllegalArgumentException("Timeout must be positive");
    }
    long budget = timeout.toNanos();
    long started = System.nanoTime();
    try {
      while (true) {
        synchronized (this) {
          refresh();
          switch (status) {
            case SUCCEEDED:
              return results;
            case FAILED:
              throw new ExecutionException(failure);
            case CANCELLED:
              throw new CancellationException("Job cancelled: " + jobId);
            case TIMED_OUT:
              throw new TimeoutException("Job timed out: " + jobId);
            default:
              break;
          }
          if (budget - (System.nanoTime() - started) <= 0) {
            TimeoutException expired = new TimeoutException("Job timed out: " + jobId);
            status = Status.TIMED_OUT;
            failure = expired;
            cancelFutures(expired);
            throw expired;
          }
        }
        for (Future<TaskOutcome> future : futures.values()) {
          if (!future.isDone()) {
            long remaining = budget - (System.nanoTime() - started);
            try {
              future.get(
                  Math.max(1, Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(50))),
                  TimeUnit.NANOSECONDS);
            } catch (TimeoutException | ExecutionException | CancellationException observed) {
              // Refresh all tasks before waiting again, including failures on other members.
            }
            break;
          }
        }
      }
    } catch (InterruptedException interrupted) {
      synchronized (this) {
        status = Status.CANCELLED;
        failure = interrupted;
        cancelFutures(interrupted);
      }
      Thread.currentThread().interrupt();
      throw interrupted;
    }
  }

  public synchronized boolean cancel() {
    refresh();
    if (status != Status.RUNNING) {
      return false;
    }
    status = Status.CANCELLED;
    CancellationException cancelled = new CancellationException("Job cancelled: " + jobId);
    failure = cancelled;
    cancelFutures(cancelled);
    if (cancelled.getSuppressed().length > 0) {
      throw cancelled;
    }
    return true;
  }

  @Override
  public void close() {
    cancel();
  }
}
