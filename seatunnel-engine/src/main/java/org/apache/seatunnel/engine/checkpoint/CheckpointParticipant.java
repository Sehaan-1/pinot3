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
package org.apache.seatunnel.engine.checkpoint;

import java.io.Serializable;
import java.util.List;

/** Caller-confined, quiesced operator adapter. Commit and completion must be idempotent. */
public interface CheckpointParticipant {
  PreparedState prepare(long checkpointId) throws Exception;
  void commit(long checkpointId, List<Serializable> handles) throws Exception;
  void complete(long checkpointId) throws Exception;
  /** Also called when prepare threw, to clean partially prepared local resources. */
  void abort(long checkpointId) throws Exception;
  void restore(Serializable state) throws Exception;
}
