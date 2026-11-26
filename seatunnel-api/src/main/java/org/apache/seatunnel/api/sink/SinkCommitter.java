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

/** External transaction coordinator. Commit and abort must tolerate repeated calls. */
public interface SinkCommitter<T extends Serializable> extends AutoCloseable {
  /** A successful return acknowledges all handles; failure is retried by the runtime. */
  void commit(long checkpointId, List<T> committables) throws Exception;

  void abort(long checkpointId, List<T> committables) throws Exception;

  @Override
  void close() throws Exception;
}
