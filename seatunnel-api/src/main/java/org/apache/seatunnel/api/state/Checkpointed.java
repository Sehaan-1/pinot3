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
package org.apache.seatunnel.api.state;

/** Snapshot lifecycle invoked serially with operator work by the execution runtime. */
public interface Checkpointed<S extends CheckpointState> {
  /** Returns a detached snapshot for the supplied monotonically increasing checkpoint ID. */
  S snapshotState(long checkpointId) throws Exception;

  /** Called after durable completion; duplicate notifications must be safe. */
  void notifyCheckpointComplete(long checkpointId) throws Exception;

  /** Releases pending resources for an aborted checkpoint. */
  void notifyCheckpointAborted(long checkpointId) throws Exception;
}
