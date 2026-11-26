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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;

/** One Avro binary record datum, not an Avro object container or registry-framed message. */
public final class AvroFormat
    implements SerializationSchema<GenericRecord>, DeserializationSchema<GenericRecord> {
  private final Schema schema;

  public AvroFormat(Schema schema) {
    this.schema = new Schema.Parser().parse(Objects.requireNonNull(schema, "schema").toString());
    if (this.schema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Avro format requires a record schema");
    }
  }

  @Override
  public byte[] serialize(GenericRecord value) throws FormatException {
    try {
      Objects.requireNonNull(value, "value");
      if (!schema.equals(value.getSchema()) || !GenericData.get().validate(schema, value)) {
        throw new IllegalArgumentException("Record does not match the configured Avro schema");
      }
      ByteArrayOutputStream output = new ByteArrayOutputStream();
      var encoder = EncoderFactory.get().binaryEncoder(output, null);
      new GenericDatumWriter<GenericRecord>(schema).write(value, encoder);
      encoder.flush();
      return output.toByteArray();
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot serialize Avro record", e);
    }
  }

  @Override
  public GenericRecord deserialize(byte[] message) throws FormatException {
    try {
      var decoder = DecoderFactory.get().binaryDecoder(Objects.requireNonNull(message, "message"), null);
      GenericRecord value = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(schema.toString()))
          .read(null, decoder);
      if (!decoder.isEnd()) {
        throw new IOException("Trailing bytes after Avro datum");
      }
      return value;
    } catch (IOException | RuntimeException e) {
      throw new FormatException("Cannot deserialize Avro record", e);
    }
  }
}
