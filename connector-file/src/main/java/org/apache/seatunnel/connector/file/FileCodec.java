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
package org.apache.seatunnel.connector.file;

import java.nio.file.Path;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Row/file adapters; established libraries own parsing and encoding. */
interface FileCodec {
  interface Reader extends AutoCloseable {
    SeaTunnelRow next() throws Exception;

    void close() throws Exception;
  }

  interface Writer extends AutoCloseable {
    void write(SeaTunnelRow row) throws Exception;

    void flush() throws Exception;

    void close() throws Exception;
  }

  static Reader reader(FileConfig config, Path path) throws Exception {
    return switch (config.format) {
      case "parquet", "orc" -> ColumnarFiles.reader(config, path);
      default -> TextFiles.reader(config, path);
    };
  }

  static Writer writer(FileConfig config, Path path) throws Exception {
    return switch (config.format) {
      case "parquet", "orc" -> ColumnarFiles.writer(config, path);
      default -> TextFiles.writer(config, path);
    };
  }
}
