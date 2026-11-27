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

import java.nio.ByteBuffer;
import java.nio.charset.*;
import java.nio.file.Path;
import java.util.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.orc.*;
import org.apache.parquet.avro.*;
import org.apache.parquet.hadoop.*;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.seatunnel.api.table.*;
import org.apache.seatunnel.api.table.CatalogTable.DataType;

/** Library-owned Parquet records and ORC vector batches. No columnar codec implementation. */
final class ColumnarFiles {
  private ColumnarFiles() {}

  static Configuration configuration() {
    return new Configuration(false);
  }

  static Schema avro(FileConfig c) {
    var fields = new ArrayList<Schema.Field>();
    for (var column : c.table.columns()) {
      var type =
          Schema.create(
              switch (column.type()) {
                case BOOLEAN -> Schema.Type.BOOLEAN;
                case INT -> Schema.Type.INT;
                case LONG -> Schema.Type.LONG;
                case FLOAT -> Schema.Type.FLOAT;
                case DOUBLE -> Schema.Type.DOUBLE;
                case STRING -> Schema.Type.STRING;
                case BYTES -> Schema.Type.BYTES;
                default -> throw FileConfig.failure();
              });
      if (column.nullable())
        type = Schema.createUnion(List.of(Schema.create(Schema.Type.NULL), type));
      fields.add(new Schema.Field(column.name(), type, null, (Object) null));
    }
    return Schema.createRecord("row", null, "org.apache.seatunnel.file", false, fields);
  }

  static TypeDescription orc(FileConfig c) {
    var schema = TypeDescription.createStruct();
    for (var column : c.table.columns())
      schema.addField(
          column.name(),
          switch (column.type()) {
            case BOOLEAN -> TypeDescription.createBoolean();
            case INT -> TypeDescription.createInt();
            case LONG -> TypeDescription.createLong();
            case FLOAT -> TypeDescription.createFloat();
            case DOUBLE -> TypeDescription.createDouble();
            case STRING -> TypeDescription.createString();
            case BYTES -> TypeDescription.createBinary();
            default -> throw FileConfig.failure();
          });
    return schema;
  }

  static org.apache.hadoop.fs.Path hpath(Path path) {
    return new org.apache.hadoop.fs.Path(path.toUri());
  }

  static RawLocalFileSystem filesystem() throws Exception {
    var fs = new RawLocalFileSystem();
    try {
      fs.initialize(java.net.URI.create("file:///"), configuration());
      return fs;
    } catch (Exception e) {
      fs.close();
      throw e;
    }
  }

  static FileCodec.Reader reader(FileConfig c, Path path) throws Exception {
    if (c.format.equals("parquet")) {
      // Validate physical/logical field schema before reading rows, including an empty file.
      var input = ParquetLocal.input(path);
      var schema = avro(c);
      try (var footer = ParquetFileReader.open(input)) {
        var expected = new AvroSchemaConverter().convert(schema);
        if (!footer.getFooter().getFileMetaData().getSchema().equals(expected))
          throw FileConfig.failure();
      }
      var reader =
          AvroParquetReader.<GenericRecord>builder(input).withConf(configuration()).build();
      return new FileCodec.Reader() {
        public SeaTunnelRow next() throws Exception {
          GenericRecord record = reader.read();
          if (record == null) return null;
          Object[] row = new Object[c.table.columns().size()];
          for (int i = 0; i < row.length; i++) {
            Object v = record.get(i);
            if (v instanceof ByteBuffer b) {
              var copy = b.duplicate();
              byte[] bytes = new byte[copy.remaining()];
              copy.get(bytes);
              v = bytes;
            } else if (v instanceof CharSequence s) v = s.toString();
            row[i] = v;
          }
          return FileRows.validate(c, new SeaTunnelRow(row));
        }

        public void close() throws Exception {
          reader.close();
        }
      };
    }
    var fs = filesystem();
    org.apache.orc.Reader reader = null;
    RecordReader records = null;
    try {
      reader =
          OrcFile.createReader(hpath(path), OrcFile.readerOptions(configuration()).filesystem(fs));
      var schema = orc(c);
      if (!reader.getSchema().equals(schema)) throw FileConfig.failure();
      records = reader.rows();
      var batch = schema.createRowBatch(256);
      var ownedReader = reader;
      var ownedRecords = records;
      return new FileCodec.Reader() {
        int index;

        public SeaTunnelRow next() throws Exception {
          if (index >= batch.size) {
            if (!ownedRecords.nextBatch(batch)) return null;
            index = 0;
          }
          Object[] row = new Object[c.table.columns().size()];
          for (int i = 0; i < row.length; i++) {
            ColumnVector vector = batch.cols[i];
            int p = vector.isRepeating ? 0 : index;
            if (!vector.noNulls && vector.isNull[p]) continue;
            row[i] =
                switch (c.table.columns().get(i).type()) {
                  case BOOLEAN -> {
                    long n = ((LongColumnVector) vector).vector[p];
                    if (n != 0 && n != 1) throw FileConfig.failure();
                    yield n == 1;
                  }
                  case INT -> Math.toIntExact(((LongColumnVector) vector).vector[p]);
                  case LONG -> ((LongColumnVector) vector).vector[p];
                  case FLOAT -> (float) ((DoubleColumnVector) vector).vector[p];
                  case DOUBLE -> ((DoubleColumnVector) vector).vector[p];
                  case STRING, BYTES -> {
                    var bytes = (BytesColumnVector) vector;
                    byte[] value =
                        Arrays.copyOfRange(
                            bytes.vector[p], bytes.start[p], bytes.start[p] + bytes.length[p]);
                    yield c.table.columns().get(i).type() == DataType.BYTES
                        ? value
                        : StandardCharsets.UTF_8
                            .newDecoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .decode(ByteBuffer.wrap(value))
                            .toString();
                  }
                  default -> throw FileConfig.failure();
                };
          }
          index++;
          return FileRows.validate(c, new SeaTunnelRow(row));
        }

        public void close() throws Exception {
          try {
            ownedRecords.close();
          } finally {
            try {
              ownedReader.close();
            } finally {
              fs.close();
            }
          }
        }
      };
    } catch (Exception e) {
      try {
        if (records != null) records.close();
      } finally {
        try {
          if (reader != null) reader.close();
        } finally {
          fs.close();
        }
      }
      throw e;
    }
  }

  static FileCodec.Writer writer(FileConfig c, Path path) throws Exception {
    if (c.format.equals("parquet")) {
      var schema = avro(c);
      var writer =
          AvroParquetWriter.<GenericRecord>builder(ParquetLocal.output(path))
              .withConf(configuration())
              .withSchema(schema)
              .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
              .withRowGroupSize(8388608L)
              .withPageSize(65536)
              .withDictionaryEncoding(false)
              .build();
      return new FileCodec.Writer() {
        public void write(SeaTunnelRow row) throws Exception {
          var record = new GenericData.Record(schema);
          for (int i = 0; i < row.getArity(); i++) {
            Object v = row.getField(i);
            record.put(i, v instanceof byte[] bytes ? ByteBuffer.wrap(bytes) : v);
          }
          writer.write(record);
        }

        public void flush() {
          /* Library row groups flush on close. */
        }

        public void close() throws Exception {
          writer.close();
        }
      };
    }
    var fs = filesystem();
    try {
      var schema = orc(c);
      var writer =
          OrcFile.createWriter(
              hpath(path),
              OrcFile.writerOptions(configuration())
                  .fileSystem(fs)
                  .setSchema(schema)
                  .overwrite(true)
                  .compress(CompressionKind.NONE)
                  .stripeSize(8388608L)
                  .bufferSize(65536));
      var batch = schema.createRowBatch(256);
      return new FileCodec.Writer() {
        public void write(SeaTunnelRow row) throws Exception {
          int p = batch.size;
          for (int i = 0; i < row.getArity(); i++) {
            Object v = row.getField(i);
            var vector = batch.cols[i];
            if (v == null) {
              vector.noNulls = false;
              vector.isNull[p] = true;
              continue;
            }
            vector.isNull[p] = false;
            switch (c.table.columns().get(i).type()) {
              case BOOLEAN -> ((LongColumnVector) vector).vector[p] = (Boolean) v ? 1 : 0;
              case INT, LONG -> ((LongColumnVector) vector).vector[p] = ((Number) v).longValue();
              case FLOAT, DOUBLE ->
                  ((DoubleColumnVector) vector).vector[p] = ((Number) v).doubleValue();
              case STRING ->
                  ((BytesColumnVector) vector)
                      .setVal(p, ((String) v).getBytes(StandardCharsets.UTF_8));
              case BYTES -> ((BytesColumnVector) vector).setVal(p, (byte[]) v);
              default -> throw FileConfig.failure();
            }
          }
          if (++batch.size == batch.getMaxSize()) flush();
        }

        public void flush() throws Exception {
          if (batch.size > 0) {
            writer.addRowBatch(batch);
            batch.reset();
          }
        }

        public void close() throws Exception {
          try {
            flush();
          } finally {
            try {
              writer.close();
            } finally {
              fs.close();
            }
          }
        }
      };
    } catch (Exception e) {
      fs.close();
      throw e;
    }
  }
}
