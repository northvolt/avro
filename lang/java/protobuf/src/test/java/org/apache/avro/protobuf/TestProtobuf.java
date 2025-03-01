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

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.protobuf.cornucopia.v1.CornucopiaTestOuterClass;
import org.apache.avro.protobuf.noopt.Test.A;
import org.apache.avro.protobuf.noopt.Test.Foo;
import org.apache.avro.protobuf.noopt.Test.M.N;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.compress.utils.Lists;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.*;

public class TestProtobuf {
  @Test
  public void testMessage() throws Exception {

    System.out.println(ProtobufData.get().getSchema(Foo.class).toString(true));
    Foo.Builder builder = Foo.newBuilder();
    builder.setInt32(0);
    builder.setInt64(2);
    builder.setUint32(3);
    builder.setUint64(4);
    builder.setSint32(5);
    builder.setSint64(6);
    builder.setFixed32(7);
    builder.setFixed64(8);
    builder.setSfixed32(9);
    builder.setSfixed64(10);
    builder.setFloat(1.0F);
    builder.setDouble(2.0);
    builder.setBool(true);
    builder.setString("foo");
    builder.setBytes(ByteString.copyFromUtf8("bar"));
    builder.setEnum(A.X);
    builder.addIntArray(27);
    builder.addSyms(A.Y);
    Foo fooInner = builder.build();

    Foo fooInArray = builder.build();
    builder = Foo.newBuilder(fooInArray);
    builder.addFooArray(fooInArray);

    com.google.protobuf.Timestamp ts = com.google.protobuf.Timestamp.newBuilder().setSeconds(1L).setNanos(2).build();
    builder.setTimestamp(ts);

    builder = Foo.newBuilder(fooInner);
    builder.setFoo(fooInner);
    Foo foo = builder.build();

    System.out.println(foo);

    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<Foo> w = new ProtobufDatumWriter<>(Foo.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(foo, e);
    e.flush();

    Object o = new ProtobufDatumReader<>(Foo.class).read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(foo, o);
  }

  @Test
  public void testMessageWithEmptyArray() throws Exception {
    Foo foo = Foo.newBuilder().setInt32(5).setBool(true).build();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<Foo> w = new ProtobufDatumWriter<>(Foo.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(foo, e);
    e.flush();
    Foo o = new ProtobufDatumReader<>(Foo.class).read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(foo.getInt32(), o.getInt32());
    assertEquals(foo.getBool(), o.getBool());
    assertEquals(0, o.getFooArrayCount());
  }

  @Test
  public void testEmptyArray() throws Exception {
    Schema s = ProtobufData.get().getSchema(Foo.class);
    assertEquals(s.getField("fooArray").defaultVal(), Lists.newArrayList());
  }

  @Test
  public void testNestedEnum() throws Exception {
    Schema s = ProtobufData.get().getSchema(N.class);
    assertEquals(N.class.getName(), SpecificData.get().getClass(s).getName());
  }

  @Test
  public void testNestedClassNamespace() throws Exception {
    Schema s = ProtobufData.get().getSchema(Foo.class);
    assertEquals(org.apache.avro.protobuf.noopt.Test.class.getName(), s.getNamespace());
  }

  @Test
  public void testClassNamespaceInMultipleFiles() throws Exception {
    Schema fooSchema = ProtobufData.get().getSchema(org.apache.avro.protobuf.multiplefiles.Foo.class);
    assertEquals(org.apache.avro.protobuf.multiplefiles.Foo.class.getPackage().getName(), fooSchema.getNamespace());

    Schema nSchema = ProtobufData.get().getSchema(org.apache.avro.protobuf.multiplefiles.M.N.class);
    assertEquals(org.apache.avro.protobuf.multiplefiles.M.class.getName(), nSchema.getNamespace());
  }

  @Test
  public void testGetNonRepeatedSchemaWithLogicalType() throws Exception {
    ProtoConversions.TimestampMillisConversion conversion = new ProtoConversions.TimestampMillisConversion();

    // Don't convert to logical type if conversion isn't set
    ProtobufData instance1 = new ProtobufData();
    Schema s1 = instance1.getSchema(com.google.protobuf.Timestamp.class);
    assertNotEquals(conversion.getRecommendedSchema(), s1);

    // Convert to logical type if conversion is set
    ProtobufData instance2 = new ProtobufData();
    instance2.addLogicalTypeConversion(conversion);
    Schema s2 = instance2.getSchema(com.google.protobuf.Timestamp.class);
    assertEquals(conversion.getRecommendedSchema(), s2);
  }

  @Test
  public void testProto3Optional() throws Exception {
    Descriptors.Descriptor d = CornucopiaTestOuterClass.CornucopiaTest.getDescriptor();
    Schema s = ProtobufData.get().getSchema(d);

    // Expected null union schemas:
    Schema intUnion = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
    Schema floatUnion = Schema
        .createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.FLOAT)));
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    GenericData.setStringType(stringSchema, GenericData.StringType.String);
    Schema stringUnion = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), stringSchema));
    Schema intTypesUnion = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
        ProtobufData.get().getSchema(CornucopiaTestOuterClass.CornucopiaTest.IntTypes.class)));

    assertEquals(intUnion, s.getField("maybe_int").schema());
    assertEquals(floatUnion, s.getField("maybe_float").schema());
    assertEquals(stringUnion, s.getField("maybe_string").schema());
    assertEquals(intTypesUnion, s.getField("it").schema());
  }

  @Test
  public void testProto3OptionalRoundTrip() throws Exception {
    // test that set optional fields round-trip correctly
    CornucopiaTestOuterClass.CornucopiaTest ct = CornucopiaTestOuterClass.CornucopiaTest.newBuilder().setMaybeInt(5)
        .setMaybeString("hello").setMaybeFloat(42.0f).build();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<CornucopiaTestOuterClass.CornucopiaTest> w = new ProtobufDatumWriter<>(
        CornucopiaTestOuterClass.CornucopiaTest.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(ct, e);
    e.flush();
    Schema s = ProtobufData.get().getSchema(ct.getDescriptorForType());
    ProtobufDatumReader<DynamicMessage> protoDatumReader = new ProtobufDatumReader<>(s);
    GenericDatumReader<GenericRecord> genericReader = new GenericDatumReader<>(protoDatumReader.getSchema());
    GenericRecord gr = genericReader.read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    assertEquals(ct.getMaybeInt(), gr.get("maybe_int"));
    assertEquals(ct.hasMaybeInt(), gr.get("maybe_int") != null);
    assertEquals(ct.getMaybeString(), gr.get("maybe_string"));
    assertEquals(ct.hasMaybeString(), gr.get("maybe_string") != null);
    assertEquals(ct.getMaybeFloat(), gr.get("maybe_float"));
    assertEquals(ct.hasMaybeFloat(), gr.get("maybe_float") != null);
  }

  @Test
  public void testProto3OptionalNull() throws Exception {
    // test that unset optional fields end up as null, not scalar default
    CornucopiaTestOuterClass.CornucopiaTest ct = CornucopiaTestOuterClass.CornucopiaTest.newBuilder().build();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    ProtobufDatumWriter<CornucopiaTestOuterClass.CornucopiaTest> w = new ProtobufDatumWriter<>(
        CornucopiaTestOuterClass.CornucopiaTest.class);
    Encoder e = EncoderFactory.get().binaryEncoder(bao, null);
    w.write(ct, e);
    e.flush();
    Schema s = ProtobufData.get().getSchema(ct.getDescriptorForType());
    ProtobufDatumReader<DynamicMessage> protoDatumReader = new ProtobufDatumReader<>(s);
    GenericDatumReader<GenericRecord> genericReader = new GenericDatumReader<>(protoDatumReader.getSchema());
    GenericRecord gr = genericReader.read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(bao.toByteArray()), null));

    // unset optional fields shall be null
    assertEquals(gr.get("maybe_int"), null);
    assertEquals(gr.get("maybe_string"), null);
    assertEquals(gr.get("maybe_float"), null);
    // but unset non-optional fields return their default value
    assertEquals(gr.get("a_bool"), ct.getABool());
  }

  @Test
  public void testStrMap() throws Exception {
    Descriptors.Descriptor d = CornucopiaTestOuterClass.CornucopiaTest.getDescriptor();
    // get the non-null part of the union schema for this type
    Schema s = ProtobufData.get().getSchema(d).getField("mt").schema().getTypes().get(1);

    Schema ss = Schema.create(Schema.Type.STRING);
    GenericData.setStringType(ss, GenericData.StringType.String);

    Schema strStrMap = Schema.createMap(ss);
    Schema strIntMap = Schema.createMap(Schema.create(Schema.Type.INT));

    assertEquals(strStrMap, s.getField("str_str_map").schema());
    assertEquals(strIntMap, s.getField("str_int_map").schema());
    assertEquals(Schema.Type.ARRAY, s.getField("int_str_map").schema().getType());
    assertEquals(Schema.Type.ARRAY, s.getField("int_int_map").schema().getType());
  }

  @Test
  public void testStrMapDatumWriting() throws Exception {
    CornucopiaTestOuterClass.CornucopiaTest.MapTypes mt = CornucopiaTestOuterClass.CornucopiaTest.MapTypes.newBuilder()
        .putIntIntMap(42, 1).putStrIntMap("Foo", 42).putStrStrMap("Bar", "Baz").putIntStrMap(0, "Bazoo").build();

    CornucopiaTestOuterClass.CornucopiaTest c = CornucopiaTestOuterClass.CornucopiaTest.newBuilder().setMt(mt).build();

    DynamicMessage dm = DynamicMessage.parseFrom(c.getDescriptorForType(), c.toByteArray());

    Schema s = ProtobufData.get().getSchema(c.getDescriptorForType());
    ProtobufDatumWriter<DynamicMessage> w = new ProtobufDatumWriter<>(s);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Encoder e = EncoderFactory.get().binaryEncoder(os, null);
    w.write(dm, e);
    e.flush();

    ProtobufDatumReader<DynamicMessage> protoDatumReader = new ProtobufDatumReader<>(s);
    GenericDatumReader<GenericRecord> genericReader = new GenericDatumReader<>(protoDatumReader.getSchema());
    GenericRecord gr = genericReader.read(null,
        DecoderFactory.get().binaryDecoder(new ByteArrayInputStream(os.toByteArray()), null));

    assertEquals(gr.getSchema(), s);

    assertTrue(((GenericData.Record) gr.get("mt")).get("str_int_map") instanceof HashMap);
    assertTrue(((GenericData.Record) gr.get("mt")).get("str_str_map") instanceof HashMap);
    assertTrue(((GenericData.Record) gr.get("mt")).get("int_str_map") instanceof GenericData.Array);
    assertTrue(((GenericData.Record) gr.get("mt")).get("int_int_map") instanceof GenericData.Array);
  }
}
