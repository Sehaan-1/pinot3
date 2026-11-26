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

import com.hazelcast.core.HazelcastInstance;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded worker-owned executor. Cancellation is cooperative, never forced thread termination. */
public final class TaskExecutionService implements AutoCloseable {
  public static final String CONTEXT_KEY = TaskExecutionService.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(TaskExecutionService.class);
  private final UUID workerId;
  private final HazelcastInstance cluster;
  private final MeterRegistry registry;
  private final List<Meter> meters = new ArrayList<>();
  private final ConcurrentHashMap<TaskKey, Work> active = new ConcurrentHashMap<>();
  private final ThreadPoolExecutor executor;
  private final Duration shutdownTimeout;
  private final Counter submitted;
  private final Counter succeeded;
  private final Counter failed;
  private final Counter cancelled;
  private final Counter rejected;
  private final Timer duration;
  private boolean closed;

  /** A null cluster is permitted for standalone local task execution; cluster tasks require a member. */
  public TaskExecutionService(UUID workerId, HazelcastInstance cluster, int threads, int queueCapacity,
      Duration shutdownTimeout, MeterRegistry registry) {
    if (threads <= 0 || queueCapacity <= 0) {
      throw new IllegalArgumentException("Worker threads and queue capacity must be positive");
    }
    if (shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
      throw new IllegalArgumentException("Shutdown timeout must be positive");
    }
    shutdownTimeout.toNanos();
    this.workerId = Objects.requireNonNull(workerId, "workerId");
    this.cluster = cluster;
    this.shutdownTimeout = shutdownTimeout;
    this.registry = Objects.requireNonNull(registry, "registry");
    AtomicInteger sequence = new AtomicInteger();
    executor = new ThreadPoolExecutor(threads, threads, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(queueCapacity), runnable -> new Thread(runnable,
            "seatunnel-task-" + workerId + "-" + sequence.incrementAndGet()),
        new ThreadPoolExecutor.AbortPolicy());
    submitted = counter("submitted");
    succeeded = counter("succeeded");
    failed = counter("failed");
    cancelled = counter("cancelled");
    rejected = counter("rejected");
    duration = Timer.builder("seatunnel.task.duration").tag("worker", workerId.toString()).register(registry);
    meters.add(duration);
    meters.add(Gauge.builder("seatunnel.task.active", executor, ThreadPoolExecutor::getActiveCount)
        .tag("worker", workerId.toString()).register(registry));
    meters.add(Gauge.builder("seatunnel.task.queued", executor, pool -> pool.getQueue().size())
        .tag("worker", workerId.toString()).register(registry));
  }

  private Counter counter(String name) {
    Counter counter = Counter.builder("seatunnel.task." + name)
        .tag("worker", workerId.toString()).register(registry);
    meters.add(counter);
    return counter;
  }

  public synchronized Future<TaskOutcome> submit(TaskKey key, EngineTask task) {
    Objects.requireNonNull(key, "key");
    Objects.requireNonNull(task, "task");
    if (closed) {
      throw new RejectedExecutionException("Worker is closed");
    }
    Work work = new Work(key, task);
    if (active.putIfAbsent(key, work) != null) {
      rejected.increment();
      throw new RejectedExecutionException("Task is already active: " + key);
    }
    try {
      executor.execute(work);
      submitted.increment();
      return work;
    } catch (RejectedExecutionException failure) {
      work.accepted = false;
      work.cancel(false);
      rejected.increment();
      throw failure;
    }
  }

  public boolean cancel(TaskKey key) {
    Work work = active.get(key);
    return work != null && work.cancel(true);
  }

  private final class Work extends FutureTask<TaskOutcome> {
    private final TaskKey key;
    private boolean accepted = true;
    private volatile boolean started;

    Work(TaskKey key, EngineTask task) {
      super(() -> {
        long started = System.nanoTime();
        try {
          task.run(new TaskContext(key, workerId, cluster));
          return new TaskOutcome(key, workerId);
        } finally {
          duration.record(System.nanoTime() - started, TimeUnit.NANOSECONDS);
        }
      });
      this.key = key;
    }

    @Override
    public void run() {
      started = true;
      try {
        super.run();
      } finally {
        active.remove(key, this);
      }
    }

    @Override
    protected void done() {
      if (!started) {
        active.remove(key, this);
      }
      executor.remove(this);
      if (!accepted) {
        return;
      }
      if (isCancelled()) {
        cancelled.increment();
      } else {
        try {
          get();
          succeeded.increment();
        } catch (ExecutionException failure) {
          failed.increment();
          LOG.debug("Task failed: {}", key, failure.getCause());
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          failed.increment();
        }
      }
    }
  }

  @Override
  public void close() {
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      List.copyOf(active.values()).forEach(work -> work.cancel(true));
      executor.shutdownNow();
    }
    try {
      if (!executor.awaitTermination(shutdownTimeout.toNanos(), TimeUnit.NANOSECONDS)) {
        throw new IllegalStateException("Worker tasks did not terminate cooperatively: " + workerId);
      }
    } catch (InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while stopping worker", interrupted);
    } finally {
      meters.forEach(registry::remove);
    }
  }
}
