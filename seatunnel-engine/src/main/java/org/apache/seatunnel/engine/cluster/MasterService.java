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

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import org.apache.seatunnel.engine.plan.PhysicalPlan;
import org.apache.seatunnel.engine.plan.TaskId;
import org.apache.seatunnel.engine.task.EngineTask;
import org.apache.seatunnel.engine.task.TaskKey;
import org.apache.seatunnel.engine.task.TaskOutcome;

/** Healthy-cluster oldest-member coordinator; no CP fencing, persistence or automatic replay. */
public final class MasterService implements AutoCloseable {
  public static final String EXECUTOR_NAME = "seatunnel-engine-dispatch";
  public static final String WORKER_ATTRIBUTE = "seatunnel.engine.worker";
  private final HazelcastInstance member;
  private final IExecutorService executor;
  private final int maxJobs;
  private final List<JobExecution> jobs = new ArrayList<>();
  private boolean closed;

  MasterService(HazelcastInstance member, int maxJobs) {
    this.member = member;
    this.maxJobs = maxJobs;
    executor = member.getExecutorService(EXECUTOR_NAME);
  }

  public boolean isLeader() {
    return member
        .getCluster()
        .getMembers()
        .iterator()
        .next()
        .getUuid()
        .equals(member.getCluster().getLocalMember().getUuid());
  }

  public JobExecution submit(PhysicalPlan plan, Map<TaskId, EngineTask> bindings) {
    return submit(plan, bindings, null);
  }

  /** Optional pinned placement; otherwise Hazelcast chooses an engine worker. */
  public synchronized JobExecution submit(
      PhysicalPlan plan, Map<TaskId, EngineTask> bindings, UUID targetId) {
    if (closed) {
      throw new RejectedExecutionException("Master service is closed");
    }
    if (!isLeader()) {
      throw new IllegalStateException("Only the oldest cluster member may submit jobs");
    }
    Objects.requireNonNull(plan, "plan");
    Map<TaskId, EngineTask> tasks = Map.copyOf(bindings);
    List<TaskId> expected =
        plan.taskGroups().stream().flatMap(group -> group.tasks().stream()).toList();
    if (tasks.size() != expected.size() || !tasks.keySet().containsAll(expected)) {
      throw new IllegalArgumentException("Bindings must match every physical task exactly");
    }
    Member target =
        targetId == null
            ? null
            : member.getCluster().getMembers().stream()
                .filter(worker -> worker.getUuid().equals(targetId))
                .filter(worker -> "true".equals(worker.getAttribute(WORKER_ATTRIBUTE)))
                .findFirst()
                .orElseThrow(
                    () -> new IllegalArgumentException("Unknown engine worker: " + targetId));
    jobs.removeIf(job -> job.status() != JobExecution.Status.RUNNING);
    if (jobs.size() >= maxJobs) {
      throw new RejectedExecutionException("Master job admission limit reached");
    }
    UUID jobId = UUID.randomUUID();
    Map<TaskId, Future<TaskOutcome>> submitted = new LinkedHashMap<>();
    try {
      for (TaskId id : expected) {
        RemoteTask task = new RemoteTask(new TaskKey(jobId, id), tasks.get(id));
        Future<TaskOutcome> future =
            target == null
                ? executor.submit(
                    task, worker -> "true".equals(worker.getAttribute(WORKER_ATTRIBUTE)))
                : executor.submitToMember(task, target);
        submitted.put(id, future);
      }
      JobExecution job = new JobExecution(jobId, submitted);
      jobs.add(job);
      return job;
    } catch (RuntimeException failure) {
      for (Future<TaskOutcome> future : submitted.values()) {
        try {
          future.cancel(true);
        } catch (RuntimeException cancellationFailure) {
          failure.addSuppressed(cancellationFailure);
        }
      }
      throw failure;
    }
  }

  @Override
  public synchronized void close() {
    if (!closed) {
      closed = true;
      RuntimeException failure = null;
      for (JobExecution job : jobs) {
        try {
          job.close();
        } catch (RuntimeException error) {
          if (failure == null) {
            failure = error;
          } else {
            failure.addSuppressed(error);
          }
        }
      }
      jobs.clear();
      // Hazelcast's executor is member-owned; never shut down the cluster-wide executor proxy here.
      if (failure != null) {
        throw failure;
      }
    }
  }
}
