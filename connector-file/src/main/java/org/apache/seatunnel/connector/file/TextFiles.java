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

import com.fasterxml.jackson.databind.*;
import java.io.*;
import java.nio.charset.*;
import java.nio.file.*;
import java.util.*;
import org.apache.commons.csv.*;
import org.apache.seatunnel.api.table.SeaTunnelRow;

/** Streaming Commons CSV and Jackson NDJSON adapters. UTF8 only, no automatic headers. */
final class TextFiles {
  private static final ObjectMapper JSON =
      new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
  private static final CSVFormat CSV = CSVFormat.RFC4180.builder().setNullString("\\N").build();

  private TextFiles() {}

  static BufferedReader input(Path path) throws IOException {
    return new BufferedReader(
        new InputStreamReader(
            Files.newInputStream(path, LinkOption.NOFOLLOW_LINKS),
            StandardCharsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)));
  }

  static FileCodec.Reader reader(FileConfig c, Path path) throws Exception {
    BufferedReader input = input(path);
    try {
      if (c.format.equals("csv")) {
        var parser = CSV.parse(input);
        var iterator = parser.iterator();
        return new FileCodec.Reader() {
          public SeaTunnelRow next() {
            if (!iterator.hasNext()) return null;
            var record = iterator.next();
            if (record.size() != c.table.columns().size()) throw FileConfig.failure();
            Object[] row = new Object[record.size()];
            for (int i = 0; i < row.length; i++) {
              String v = record.get(i);
              row[i] = v == null ? null : FileRows.parse(v, c.table.columns().get(i).type());
            }
            return FileRows.validate(c, new SeaTunnelRow(row));
          }

          public void close() throws Exception {
            parser.close();
          }
        };
      }
      return new FileCodec.Reader() {
        public SeaTunnelRow next() throws Exception {
          String line = input.readLine();
          if (line == null) return null;
          if (line.getBytes(StandardCharsets.UTF_8).length > c.maxRow * 2 + 4096)
            throw FileConfig.failure();
          if (c.format.equals("text")) return FileRows.validate(c, new SeaTunnelRow(line));
          JsonNode tree = JSON.readTree(line);
          if (tree == null || !tree.isArray() || tree.size() != c.table.columns().size())
            throw FileConfig.failure();
          Object[] row = new Object[tree.size()];
          for (int i = 0; i < row.length; i++) {
            var value = tree.get(i);
            var type = c.table.columns().get(i).type();
            if (value.isNull()) continue;
            row[i] =
                switch (type) {
                  case STRING, BYTES -> {
                    if (!value.isTextual()) throw FileConfig.failure();
                    yield FileRows.parse(value.textValue(), type);
                  }
                  case BOOLEAN -> {
                    if (!value.isBoolean()) throw FileConfig.failure();
                    yield value.booleanValue();
                  }
                  case INT -> {
                    if (!value.isIntegralNumber() || !value.canConvertToInt())
                      throw FileConfig.failure();
                    yield value.intValue();
                  }
                  case LONG -> {
                    if (!value.isIntegralNumber() || !value.canConvertToLong())
                      throw FileConfig.failure();
                    yield value.longValue();
                  }
                  case FLOAT -> {
                    if (!value.isNumber()) throw FileConfig.failure();
                    yield value.floatValue();
                  }
                  case DOUBLE -> {
                    if (!value.isNumber()) throw FileConfig.failure();
                    yield value.doubleValue();
                  }
                  default -> throw FileConfig.failure();
                };
          }
          return FileRows.validate(c, new SeaTunnelRow(row));
        }

        public void close() throws Exception {
          input.close();
        }
      };
    } catch (Exception e) {
      input.close();
      throw e;
    }
  }

  static FileCodec.Writer writer(FileConfig c, Path path) throws Exception {
    var output =
        new BufferedWriter(
            new OutputStreamWriter(
                Files.newOutputStream(
                    path,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    LinkOption.NOFOLLOW_LINKS),
                StandardCharsets.UTF_8
                    .newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)));
    try {
      if (c.format.equals("csv")) {
        var printer = new CSVPrinter(output, CSV);
        return new FileCodec.Writer() {
          public void write(SeaTunnelRow row) throws Exception {
            var fields = new ArrayList<String>();
            for (int i = 0; i < row.getArity(); i++) {
              Object v = row.getField(i);
              String field = v == null ? null : FileRows.print(v);
              if ("\\N".equals(field)) throw FileConfig.failure();
              fields.add(field);
            }
            printer.printRecord(fields);
          }

          public void flush() throws Exception {
            printer.flush();
          }

          public void close() throws Exception {
            printer.close();
          }
        };
      }
      return new FileCodec.Writer() {
        public void write(SeaTunnelRow row) throws Exception {
          if (c.format.equals("text")) {
            String s = (String) row.getField(0);
            if (s.contains("\n") || s.contains("\r")) throw FileConfig.failure();
            output.write(s);
          } else {
            Object[] values = new Object[row.getArity()];
            for (int i = 0; i < values.length; i++) values[i] = row.getField(i);
            output.write(JSON.writeValueAsString(values));
          }
          output.newLine();
        }

        public void flush() throws Exception {
          output.flush();
        }

        public void close() throws Exception {
          output.close();
        }
      };
    } catch (Exception e) {
      output.close();
      throw e;
    }
  }
}
