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

import com.google.common.io.Closer;
import com.hazelcast.config.Config;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import org.apache.seatunnel.engine.task.TaskExecutionService;

/** Owns one embedded member and its execution resources. Trusted private networks only. */
public final class EngineNode implements Closeable {
  private final HazelcastInstance member;
  private final MasterService master;
  private final Closer resources;
  private boolean closed;

  private EngineNode(HazelcastInstance member, MasterService master, Closer resources) {
    this.member = member;
    this.master = master;
    this.resources = resources;
  }

  public static EngineNode start(EngineSettings settings) throws IOException {
    return start(settings, new SimpleMeterRegistry(), true);
  }

  public static EngineNode start(EngineSettings settings, MeterRegistry registry)
      throws IOException {
    return start(settings, registry, false);
  }

  private static EngineNode start(
      EngineSettings settings, MeterRegistry registry, boolean ownRegistry) throws IOException {
    Objects.requireNonNull(settings, "settings");
    Objects.requireNonNull(registry, "registry");
    Closer closer = Closer.create();
    try {
      if (ownRegistry) {
        closer.register((Closeable) registry::close);
      }
      Config config = new Config().setClusterName(settings.clusterName());
      config.addMapConfig(org.apache.seatunnel.engine.checkpoint.CheckpointStore.mapConfig());
      config.getJetConfig().setEnabled(false);
      config.setProperty("hazelcast.phone.home.enabled", "false");
      config.setProperty("hazelcast.shutdownhook.enabled", "false");
      config.setProperty("hazelcast.logging.type", "slf4j");
      config.setProperty("hazelcast.operation.thread.count", "2");
      config.setProperty("hazelcast.operation.generic.thread.count", "2");
      config.setProperty("hazelcast.io.input.thread.count", "1");
      config.setProperty("hazelcast.io.output.thread.count", "1");
      config.setProperty("hazelcast.wait.seconds.before.join", "0");
      config.getMemberAttributeConfig().setAttribute(MasterService.WORKER_ATTRIBUTE, "true");
      var network = config.getNetworkConfig();
      network.setPort(settings.port()).setPortAutoIncrement(false);
      network.getInterfaces().setEnabled(true).addInterface(settings.bindAddress());
      var join = network.getJoin();
      join.getAutoDetectionConfig().setEnabled(false);
      join.getMulticastConfig().setEnabled(false);
      join.getTcpIpConfig().setEnabled(true).setMembers(settings.seeds());
      config.addExecutorConfig(
          new ExecutorConfig(MasterService.EXECUTOR_NAME)
              .setPoolSize(settings.workerThreads())
              .setQueueCapacity(settings.queueCapacity()));
      HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
      closer.register((Closeable) member::shutdown);
      TaskExecutionService worker =
          new TaskExecutionService(
              member.getCluster().getLocalMember().getUuid(),
              member,
              settings.workerThreads(),
              settings.queueCapacity(),
              settings.shutdownTimeout(),
              registry);
      closer.register((Closeable) worker::close);
      member.getUserContext().put(TaskExecutionService.CONTEXT_KEY, worker);
      closer.register(
          (Closeable)
              () -> member.getUserContext().remove(TaskExecutionService.CONTEXT_KEY, worker));
      MasterService master = new MasterService(member, settings.maxJobs());
      closer.register((Closeable) master::close);
      return new EngineNode(member, master, closer);
    } catch (Throwable failure) {
      try {
        closer.close();
      } catch (Throwable cleanupFailure) {
        failure.addSuppressed(cleanupFailure);
      }
      throw closer.rethrow(failure);
    }
  }

  public synchronized HazelcastInstance member() {
    ensureOpen();
    return member;
  }

  public synchronized MasterService master() {
    ensureOpen();
    return master;
  }

  private void ensureOpen() {
    if (closed) {
      throw new IllegalStateException("Engine node is closed");
    }
  }

  @Override
  public synchronized void close() throws IOException {
    if (!closed) {
      closed = true;
      resources.close();
    }
  }
}
