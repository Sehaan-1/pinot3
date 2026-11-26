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
package org.apache.seatunnel.api.source;

import java.util.List;
import org.apache.seatunnel.api.state.CheckpointState;
import org.apache.seatunnel.api.state.Checkpointed;

/** Serially invoked source-work coordinator; no engine-specific context leaks into the SPI. */
public interface SplitEnumerator<S extends SourceSplit, C extends CheckpointState>
    extends AutoCloseable, Checkpointed<C> {
  void start() throws Exception;

  void registerReader(int readerId) throws Exception;

  void handleSplitRequest(int readerId) throws Exception;

  void addSplitsBack(List<S> splits, int readerId) throws Exception;

  @Override
  void close() throws Exception;

  interface Context<S extends SourceSplit> {
    int currentParallelism();

    void assignSplits(int readerId, List<S> splits) throws Exception;

    void signalNoMoreSplits(int readerId) throws Exception;
  }
}
