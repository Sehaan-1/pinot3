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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import org.apache.seatunnel.engine.task.EngineTask;
import org.apache.seatunnel.engine.task.TaskExecutionService;
import org.apache.seatunnel.engine.task.TaskKey;
import org.apache.seatunnel.engine.task.TaskOutcome;

/** Hazelcast owns delivery and cancellation; local worker execution owns user task threads. */
final class RemoteTask implements Callable<TaskOutcome>, Serializable, HazelcastInstanceAware {
  private final TaskKey key;
  private final EngineTask task;
  private transient HazelcastInstance instance;

  RemoteTask(TaskKey key, EngineTask task) {
    this.key = key;
    this.task = task;
  }

  @Override
  public void setHazelcastInstance(HazelcastInstance instance) {
    this.instance = instance;
  }

  @Override
  public TaskOutcome call() throws Exception {
    Object context = instance.getUserContext().get(TaskExecutionService.CONTEXT_KEY);
    if (!(context instanceof TaskExecutionService worker)) {
      throw new IllegalStateException("Engine worker is not ready or is shutting down");
    }
    var future = worker.submit(key, task);
    try {
      return future.get();
    } catch (InterruptedException interrupted) {
      future.cancel(true);
      Thread.currentThread().interrupt();
      throw interrupted;
    } catch (CancellationException cancelled) {
      throw new IllegalStateException(
          "Worker task was cancelled or worker is shutting down", cancelled);
    } catch (ExecutionException failure) {
      Throwable cause = failure.getCause();
      if (cause instanceof Exception exception) {
        throw exception;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw failure;
    }
  }
}
