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
package org.apache.seatunnel.format;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.Util;

/** Thrift FileMetaData payload only: this is NOT a Parquet file or row codec. */
public final class ParquetMetadataFormat
    implements SerializationSchema<FileMetaData>, DeserializationSchema<FileMetaData> {
  @Override
  public byte[] serialize(FileMetaData value) throws FormatException {
    try {
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      Util.writeFileMetaData(Objects.requireNonNull(value, "value"), output);
      return output.toByteArray();
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot serialize Parquet metadata", e);
    }
  }

  @Override
  public FileMetaData deserialize(byte[] message) throws FormatException {
    try {
      ByteArrayInputStream input = new ByteArrayInputStream(Objects.requireNonNull(message, "message"));
      FileMetaData value = Util.readFileMetaData(input);
      if (input.available() != 0) {
        throw new IOException("Trailing bytes after Parquet metadata");
      }
      return value;
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot deserialize Parquet metadata", e);
    }
  }
}
