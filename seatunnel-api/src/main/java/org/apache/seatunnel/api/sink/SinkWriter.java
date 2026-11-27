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
import java.util.List;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.state.Checkpointed;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Single-thread-confined writer; prepareCommit must not commit external transactions. */
public interface SinkWriter<C extends CheckpointState, T extends Serializable>
    extends AutoCloseable, Checkpointed<C> {
  void write(SeaTunnelRow row) throws Exception;

  /** Flushes buffered writes; endOfInput marks the final flush of a bounded input. */
  void flush(boolean endOfInput) throws Exception;

  /**
   * Returns detached commit handles for the checkpoint, after flushing its data. Transactional
   * adapters must make these independently recoverable by a fresh committer. Closing a writer must
   * not invalidate prepared handles in a published or publication-uncertain decision. Abort
   * notifications must also clean partial preparation when this method throws before returning.
   */
  List<T> prepareCommit(long checkpointId) throws Exception;

  @Override
  void close() throws Exception;
}
