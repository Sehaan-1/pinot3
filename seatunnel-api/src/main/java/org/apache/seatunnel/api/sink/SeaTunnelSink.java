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
package org.apache.seatunnel.api.sink;

import java.io.Serializable;
import java.util.Optional;
import org.apache.seatunnel.api.state.CheckpointState;

/** Configured sink factory with optional external transaction support. */
public interface SeaTunnelSink<C extends CheckpointState, T extends Serializable> {
  SinkWriter<C, T> createWriter(int writerId, Optional<C> restoredState) throws Exception;

  /** Empty for non-transactional sinks; those sinks do not claim exactly-once delivery. */
  Optional<SinkCommitter<T>> createCommitter() throws Exception;
}
