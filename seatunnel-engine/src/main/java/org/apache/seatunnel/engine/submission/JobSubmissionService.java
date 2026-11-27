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
package org.apache.seatunnel.engine.submission;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;
import org.apache.seatunnel.engine.cluster.JobExecution;
import org.apache.seatunnel.engine.cluster.MasterService;

/** Bounded, caller-polled job ownership. No persistence, background supervisor or retry. */
public final class JobSubmissionService implements AutoCloseable {
  public record JobView(UUID jobId, JobExecution.Status status) {}

  private final MasterService master;
  private final Map<String, JobDefinition> definitions;
  private final int capacity;
  private final Map<UUID, JobExecution> jobs = new LinkedHashMap<>();
  private boolean closed;

  public JobSubmissionService(
      MasterService master, Map<String, JobDefinition> definitions, int capacity) {
    this.master = Objects.requireNonNull(master, "master");
    this.definitions = Map.copyOf(definitions);
    if (capacity <= 0 || capacity > 128 || definitions.isEmpty())
      throw new IllegalArgumentException("Positive capacity and installed definitions required");
    definitions.keySet().forEach(JobSubmissionService::validateDefinition);
    this.capacity = capacity;
  }

  public static void validateDefinition(String id) {
    if (id == null || !id.matches("[a-z][a-z0-9-]{0,63}"))
      throw new IllegalArgumentException("Invalid definition ID");
  }

  public static Map<String, String> validateParameters(Map<String, String> parameters) {
    if (parameters == null || parameters.size() > 64)
      throw new IllegalArgumentException("At most 64 parameters required");
    parameters.forEach(
        (key, value) -> {
          if (key == null
              || !key.matches("[a-zA-Z][a-zA-Z0-9_.-]{0,63}")
              || value == null
              || value.length() > 4096)
            throw new IllegalArgumentException("Invalid parameter key or value");
        });
    return Map.copyOf(parameters);
  }

  public synchronized JobView submit(String definition, Map<String, String> parameters) {
    ensureOpen();
    validateDefinition(definition);
    Map<String, String> validated = validateParameters(parameters);
    JobDefinition factory = definitions.get(definition);
    if (factory == null)
      throw new SubmissionException(SubmissionException.Reason.NOT_FOUND, "Unknown definition");
    if (jobs.size() >= capacity)
      throw conflict("Job retention capacity reached; delete terminal jobs");
    if (!master.isLeader()) throw conflict("Submit to the oldest cluster member");
    SubmissionPlan submission = Objects.requireNonNull(factory.create(validated), "submission");
    JobExecution job;
    try {
      job = master.submit(submission.plan(), submission.bindings(), submission.target());
    } catch (RejectedExecutionException | IllegalStateException unavailable) {
      throw conflict("Master is unavailable or at capacity");
    }
    jobs.put(job.jobId(), job);
    return new JobView(job.jobId(), JobExecution.Status.RUNNING);
  }

  /** Bounded discovery for inspecting potentially ambiguous submission outcomes. */
  public synchronized java.util.List<JobView> list() {
    ensureOpen();
    return jobs.values().stream().map(job -> new JobView(job.jobId(), job.status())).toList();
  }

  public synchronized JobView status(UUID id) {
    JobExecution job = find(id);
    return new JobView(id, job.status());
  }

  public synchronized JobView cancel(UUID id) {
    JobExecution job = find(id);
    job.cancel();
    return new JobView(id, job.status());
  }

  public synchronized void delete(UUID id) {
    JobExecution job = find(id);
    if (job.status() == JobExecution.Status.RUNNING)
      throw conflict("Cancel or finish job before deleting");
    job.close();
    jobs.remove(id);
  }

  private JobExecution find(UUID id) {
    ensureOpen();
    JobExecution job = jobs.get(Objects.requireNonNull(id, "jobId"));
    if (job == null)
      throw new SubmissionException(SubmissionException.Reason.NOT_FOUND, "Unknown job");
    return job;
  }

  private void ensureOpen() {
    if (closed) throw conflict("Submission service is closed");
  }

  private static SubmissionException conflict(String message) {
    return new SubmissionException(SubmissionException.Reason.CONFLICT, message);
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    RuntimeException failure = null;
    for (JobExecution job : jobs.values()) {
      try {
        job.close();
      } catch (RuntimeException error) {
        if (failure == null) failure = error;
        else if (failure != error) failure.addSuppressed(error);
      }
    }
    jobs.clear();
    if (failure != null) throw failure;
  }
}
