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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.core.type.TypeReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.format.FileMetaData;
import org.apache.parquet.format.SchemaElement;
import org.junit.jupiter.api.Test;

class FormatsTest {
  private static final String AVRO_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Event\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"}]}";

  @Test
  void jsonUsesTypedValuesAndRejectsMalformedTrailingAndNullMessages() throws Exception {
    JsonFormat<Map<String, Integer>> format = new JsonFormat<>(new TypeReference<>() {});
    assertThat(format.deserialize(format.serialize(Map.of("id", 42)))).containsEntry("id", 42);
    for (String text : List.of("{bad", "{} {}", "null", "")) {
      assertThatThrownBy(() -> format.deserialize(text.getBytes(StandardCharsets.UTF_8)))
          .isInstanceOf(FormatException.class).hasCauseInstanceOf(Exception.class);
    }
    assertThat(new JsonFormat<>(String.class).deserialize("\"hello\"".getBytes(StandardCharsets.UTF_8)))
        .isEqualTo("hello");
    assertThatThrownBy(() -> format.serialize(null)).isInstanceOf(FormatException.class);
  }

  @Test
  void csvPreservesQuotesUnicodeEmptyStringsAndMultilineFields() throws Exception {
    CsvFormat format = new CsvFormat();
    List<String> fields = List.of("a,b", "a\"b", "line1\r\nline2", "", "नमस्ते");
    assertThat(format.deserialize(format.serialize(fields))).isEqualTo(fields);
    assertThat(format.deserialize(format.serialize(List.of("")))).containsExactly("");
    assertThat(new String(format.serialize(List.of("a", "b")), StandardCharsets.UTF_8))
        .isEqualTo("a,b\r\n");
  }

  @Test
  void csvRejectsAmbiguousNullsMultipleRecordsAndInvalidUtf8() {
    CsvFormat format = new CsvFormat();
    assertThatThrownBy(() -> format.serialize(Arrays.asList("a", null)))
        .isInstanceOf(FormatException.class);
    assertThatThrownBy(() -> format.serialize(List.of())).isInstanceOf(FormatException.class);
    for (String text : List.of("", "a\nb\n", "\"unclosed")) {
      assertThatThrownBy(() -> format.deserialize(text.getBytes(StandardCharsets.UTF_8)))
          .isInstanceOf(FormatException.class);
    }
    assertThatThrownBy(() -> format.deserialize(new byte[] {(byte) 0xff}))
        .isInstanceOf(FormatException.class);
  }

  @Test
  void avroRoundTripsWithTheConfiguredSchema() throws Exception {
    Schema schema = new Schema.Parser().parse(AVRO_SCHEMA);
    AvroFormat format = new AvroFormat(schema);
    GenericRecord record = new GenericData.Record(schema);
    record.put("id", 17);
    assertThat(format.deserialize(format.serialize(record)).get("id")).isEqualTo(17);
    record.put("id", "not-an-int");
    assertThatThrownBy(() -> format.serialize(record)).isInstanceOf(FormatException.class);
    assertThatThrownBy(() -> format.deserialize(new byte[0])).isInstanceOf(FormatException.class);
    record.put("id", 1);
    byte[] valid = format.serialize(record);
    byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
    assertThatThrownBy(() -> format.deserialize(trailing)).isInstanceOf(FormatException.class);
    assertThatThrownBy(() -> new AvroFormat(Schema.create(Schema.Type.STRING)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  private FileMetaData metadata() {
    SchemaElement root = new SchemaElement("schema").setNum_children(0);
    return new FileMetaData(1, List.of(root), 0, List.of());
  }

  @Test
  void parquetMetadataRoundTripsWithoutClaimingToBeAFileCodec() throws Exception {
    ParquetMetadataFormat format = new ParquetMetadataFormat();
    FileMetaData metadata = metadata().setCreated_by("seatunnel-test");
    assertThat(format.deserialize(format.serialize(metadata))).isEqualTo(metadata);
    assertThatThrownBy(() -> format.deserialize(new byte[0])).isInstanceOf(FormatException.class);
    byte[] encoded = format.serialize(metadata);
    assertThatThrownBy(() -> format.deserialize(Arrays.copyOf(encoded, encoded.length + 1)))
        .isInstanceOf(FormatException.class);
    assertThatThrownBy(() -> format.serialize(null)).isInstanceOf(FormatException.class);
  }

  @Test
  void codecsCanBeReusedConcurrently() throws Exception {
    JsonFormat<String> json = new JsonFormat<>(String.class);
    CsvFormat csv = new CsvFormat();
    AvroFormat avro = new AvroFormat(new Schema.Parser().parse(AVRO_SCHEMA));
    ParquetMetadataFormat parquet = new ParquetMetadataFormat();
    var executor = Executors.newFixedThreadPool(4);
    try {
      List<Callable<Boolean>> calls = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        final int id = i;
        calls.add(() -> {
          GenericRecord row = new GenericData.Record(new Schema.Parser().parse(AVRO_SCHEMA));
          row.put("id", id);
          return json.deserialize(json.serialize("v" + id)).equals("v" + id)
              && csv.deserialize(csv.serialize(List.of("v," + id))).equals(List.of("v," + id))
              && avro.deserialize(avro.serialize(row)).get("id").equals(id)
              && parquet.deserialize(parquet.serialize(metadata())).equals(metadata());
        });
      }
      for (var result : executor.invokeAll(calls)) {
        assertThat(result.get()).isTrue();
      }
    } finally {
      executor.shutdownNow();
    }
  }
}
