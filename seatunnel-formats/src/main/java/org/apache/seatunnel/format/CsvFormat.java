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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/** One UTF-8 RFC4180 record, without headers; null fields and empty field lists are rejected. */
public final class CsvFormat
    implements SerializationSchema<List<String>>, DeserializationSchema<List<String>> {
  @Override
  public byte[] serialize(List<String> value) throws FormatException {
    try {
      List<String> fields = List.copyOf(value);
      if (fields.isEmpty()) {
        throw new IllegalArgumentException("CSV record must have at least one field");
      }
      StringWriter output = new StringWriter();
      try (CSVPrinter printer = new CSVPrinter(output, CSVFormat.RFC4180)) {
        printer.printRecord(fields);
      }
      return output.toString().getBytes(StandardCharsets.UTF_8);
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot serialize CSV record", e);
    }
  }

  @Override
  public List<String> deserialize(byte[] message) throws FormatException {
    try {
      String text =
          StandardCharsets.UTF_8
              .newDecoder()
              .onMalformedInput(CodingErrorAction.REPORT)
              .onUnmappableCharacter(CodingErrorAction.REPORT)
              .decode(ByteBuffer.wrap(Objects.requireNonNull(message, "message")))
              .toString();
      try (CSVParser parser = CSVFormat.RFC4180.parse(new StringReader(text))) {
        List<CSVRecord> records = parser.getRecords();
        if (records.size() != 1) {
          throw new IllegalArgumentException("Expected exactly one CSV record");
        }
        return StreamSupport.stream(records.get(0).spliterator(), false).toList();
      }
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot deserialize CSV record", e);
    }
  }
}
