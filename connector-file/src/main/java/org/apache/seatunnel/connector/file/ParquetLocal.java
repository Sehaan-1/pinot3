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

import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.nio.file.*;
import org.apache.parquet.io.*;

/** Minimal NIO stream adaptation for Parquet's public filesystem-neutral interfaces. */
final class ParquetLocal {
  private ParquetLocal() {}

  static InputFile input(Path path) {
    return new InputFile() {
      public long getLength() throws IOException {
        return Files.size(path);
      }

      public SeekableInputStream newStream() throws IOException {
        var channel = FileChannel.open(path, StandardOpenOption.READ, LinkOption.NOFOLLOW_LINKS);
        return new SeekableInputStream() {
          public long getPos() throws IOException {
            return channel.position();
          }

          public void seek(long position) throws IOException {
            channel.position(position);
          }

          public int read() throws IOException {
            byte[] one = new byte[1];
            return read(one, 0, 1) < 0 ? -1 : one[0] & 255;
          }

          public int read(byte[] b, int off, int len) throws IOException {
            return channel.read(ByteBuffer.wrap(b, off, len));
          }

          public int read(ByteBuffer b) throws IOException {
            return channel.read(b);
          }

          public void readFully(byte[] b) throws IOException {
            readFully(b, 0, b.length);
          }

          public void readFully(byte[] b, int off, int len) throws IOException {
            readFully(ByteBuffer.wrap(b, off, len));
          }

          public void readFully(ByteBuffer b) throws IOException {
            while (b.hasRemaining())
              if (channel.read(b) < 0) throw new EOFException("Truncated Parquet input");
          }

          public void close() throws IOException {
            channel.close();
          }
        };
      }
    };
  }

  static OutputFile output(Path path) {
    return new OutputFile() {
      public PositionOutputStream create(long block) throws IOException {
        return open(false);
      }

      public PositionOutputStream createOrOverwrite(long block) throws IOException {
        return open(true);
      }

      private PositionOutputStream open(boolean overwrite) throws IOException {
        // Only a connector-owned, already-created staging path is passed here.
        var channel =
            FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING,
                LinkOption.NOFOLLOW_LINKS);
        return new PositionOutputStream() {
          public long getPos() throws IOException {
            return channel.position();
          }

          public void write(int value) throws IOException {
            write(new byte[] {(byte) value}, 0, 1);
          }

          public void write(byte[] b, int off, int len) throws IOException {
            var buffer = ByteBuffer.wrap(b, off, len);
            while (buffer.hasRemaining()) channel.write(buffer);
          }

          public void close() throws IOException {
            channel.close();
          }
        };
      }

      public boolean supportsBlockSize() {
        return false;
      }

      public long defaultBlockSize() {
        return 0;
      }
    };
  }
}
