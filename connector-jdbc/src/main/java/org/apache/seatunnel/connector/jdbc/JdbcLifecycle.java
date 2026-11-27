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
package org.apache.seatunnel.connector.jdbc;

/** No fake checkpoint support; one-shot, single-thread-confined runtime owner. */
abstract class JdbcLifecycle {
  boolean started, closed, failed;

  final void active() {
    if (!started || closed || failed)
      throw new IllegalStateException("JDBC operator is not active");
  }

  final void begin() {
    if (started || closed || failed)
      throw new IllegalStateException("JDBC operator cannot restart");
    started = true;
  }

  public final JdbcSource.State snapshotState(long id) {
    throw new UnsupportedOperationException("JDBC checkpoint/restore is not supported");
  }

  public final void notifyCheckpointComplete(long id) {
    throw new UnsupportedOperationException("JDBC checkpoints are not supported");
  }

  public final void notifyCheckpointAborted(long id) {
    throw new UnsupportedOperationException("JDBC checkpoints are not supported");
  }
}
