/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.protobuf;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ConcurrentModificationException;
import java.util.List;

import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.Encoder;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.EnumValueDescriptor;

/**
 * {@link org.apache.avro.io.DatumWriter DatumWriter} for generated protobuf
 * classes.
 */
public class ProtobufDatumWriter<T> extends GenericDatumWriter<T> {
  public ProtobufDatumWriter() {
    super(ProtobufData.get());
  }

  public ProtobufDatumWriter(Class<T> c) {
    super(ProtobufData.get().getSchema(c), ProtobufData.get());
  }

  public ProtobufDatumWriter(Schema schema) {
    super(schema, ProtobufData.get());
  }

  protected ProtobufDatumWriter(Schema root, ProtobufData protobufData) {
    super(root, protobufData);
  }

  protected ProtobufDatumWriter(ProtobufData protobufData) {
    super(protobufData);
  }

  @Override
  protected void writeEnum(Schema schema, Object datum, Encoder out) throws IOException {
    if (!(datum instanceof EnumValueDescriptor))
      super.writeEnum(schema, datum, out); // punt to generic
    else
      out.writeEnum(schema.getEnumOrdinal(((EnumValueDescriptor) datum).getName()));
  }

  @Override
  protected void writeBytes(Object datum, Encoder out) throws IOException {
    ByteString bytes = (ByteString) datum;
    out.writeBytes(bytes.toByteArray(), 0, bytes.size());
  }

  @Override
  protected void writeMap(Schema schema, Object datum, Encoder out) throws IOException {
    if (datum instanceof List<?> && ((List<?>) datum).size() > 0
        && ((List<?>) datum).get(0) instanceof DynamicMessage) {
      List<?> mapRecords = (List<?>) datum;
      Schema value = schema.getValueType();

      int size = mapRecords.size();
      int actualSize = 0;
      out.writeMapStart();
      out.setItemCount(size);
      for (Object entry : mapRecords) {
        AbstractMap.SimpleEntry<String, Object> r = getStrMapRecord(entry);
        out.startItem();
        writeString(r.getKey(), out);
        write(value, r.getValue(), out);
        actualSize++;
      }
      out.writeMapEnd();
      if (actualSize != size) {
        throw new ConcurrentModificationException(
            "Size of map written was " + size + ", but number of entries written was " + actualSize + ". ");
      }
    } else {
      super.writeMap(schema, datum, out);
    }
  }

  private AbstractMap.SimpleEntry<String, Object> getStrMapRecord(Object mapRecord) {
    if (mapRecord instanceof DynamicMessage) {
      DynamicMessage dm = (DynamicMessage) mapRecord;
      if (dm.getDescriptorForType().getOptions().hasMapEntry()
          && dm.getDescriptorForType().getOptions().getMapEntry()) {
        Descriptors.FieldDescriptor keyDesc = dm.getDescriptorForType().getFields().get(0);
        Descriptors.FieldDescriptor valDesc = dm.getDescriptorForType().getFields().get(1);
        if (keyDesc.getType() == Descriptors.FieldDescriptor.Type.STRING) {
          return new AbstractMap.SimpleEntry<>(dm.getField(keyDesc).toString(), dm.getField(valDesc));
        }
      }
    }
    throw new AvroRuntimeException("Not a string map record: " + mapRecord);
  }
}
