/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.ProtobufUtil;
import org.infinispan.protostream.RawProtoStreamReader;
import org.infinispan.protostream.RawProtoStreamWriter;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.impl.RawProtoStreamReaderImpl;
import org.infinispan.protostream.impl.RawProtoStreamWriterImpl;
import org.junit.Test;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.infinispan.api.InfinispanDocument;
import org.teiid.infinispan.api.TableWireFormat;
import org.teiid.language.Select;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.document.Document;
import org.teiid.translator.marshallers.G1;
import org.teiid.translator.marshallers.G1Marshaller;
import org.teiid.translator.marshallers.G2;
import org.teiid.translator.marshallers.G2Marshaller;
import org.teiid.translator.marshallers.G3;
import org.teiid.translator.marshallers.G3Marshaller;
import org.teiid.translator.marshallers.G4;
import org.teiid.translator.marshallers.G4Marshaller;

public class TestTeiidTableMarsheller {

    private IckleConversionVisitor helpExecute(String query) throws Exception {
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        //System.out.println(DDLStringVisitor.getDDLString(mf.getSchema(), null, null));
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = TestProtobufMetadataProcessor.getTransformationMetadata(mf, ef);
        TranslationUtility utility = new TranslationUtility(metadata);
        Select cmd = (Select)utility.parseCommand(query);
        RuntimeMetadata runtimeMetadata = new RuntimeMetadataImpl(metadata);
        IckleConversionVisitor visitor = new IckleConversionVisitor(runtimeMetadata, false);
        visitor.visitNode(cmd);
        visitor.getQuery();
        return visitor;
    }

    @Test
    public void testReadSimple() throws Exception {
        IckleConversionVisitor visitor = helpExecute("select * from G1");

        TeiidTableMarsheller g1ReadMarshaller = new TeiidTableMarsheller(
                ProtobufMetadataProcessor.getMessageName(visitor.getParentTable()),
                MarshallerBuilder.getWireMap(visitor.getParentTable(), visitor.getMetadata()));

        G1Marshaller g1WriteMarshller = new G1Marshaller() {
            @Override
            public G1 readFrom(ProtoStreamReader reader) throws IOException {
                throw new RuntimeException("Use Teiid marshaller for reading for this test..");
            }
        };

        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ctx.registerProtoFiles(FileDescriptorSource.fromString("tables.proto",
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));

        G1 g1 = buildG1();
        ctx.registerMarshaller(g1WriteMarshller);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(baos);
        WrappedMessage.writeMessage(ctx, out, g1);
        out.flush();
        baos.flush();
        ctx.unregisterMarshaller(g1WriteMarshller);

        ctx.registerMarshaller(g1ReadMarshaller);
        RawProtoStreamReader in = RawProtoStreamReaderImpl.newInstance(baos.toByteArray());
        Document result = WrappedMessage.readMessage(ctx, in);
        Map<String, Object> row = result.flatten().get(0);

        assertEquals(1, row.get("e1"));
        assertEquals("foo", row.get("e2"));
        assertEquals(1.234f, row.get("e3"));
        assertNull(row.get("e4"));
        List<String> e5 = (List<String>)row.get("e5");
        assertArrayEquals(new String[] {"hello", "world"}, e5.toArray(new String[e5.size()]));
        ctx.unregisterMarshaller(g1ReadMarshaller);
    }

    @Test
    public void testWriteSimple() throws Exception {
        IckleConversionVisitor visitor = helpExecute("select * from G1");

        TeiidTableMarsheller g1WriteMarshaller = new TeiidTableMarsheller(
                ProtobufMetadataProcessor.getMessageName(visitor.getParentTable()),
                MarshallerBuilder.getWireMap(visitor.getParentTable(), visitor.getMetadata()));

        G1Marshaller g1ReadMarshaller = new G1Marshaller() {
            @Override
            public void writeTo(ProtoStreamWriter writer, G1 g1) throws IOException {
                throw new RuntimeException("Use Teiid marshaller for writing for this test..");
            }
        };

        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ctx.registerProtoFiles(FileDescriptorSource.fromString("tables.proto",
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));


        InfinispanDocument g1 = new InfinispanDocument("pm1.G1",
                MarshallerBuilder.getWireMap(visitor.getParentTable(), visitor.getMetadata()), null);
        g1.addProperty("e1", 1);
        g1.addProperty("e2", "foo");
        g1.addProperty("e3", 1.234f);
        g1.addProperty("e4", null);
        g1.addArrayProperty("e5", "hello");
        g1.addArrayProperty("e5", "world");

        // write to buffer
        ctx.registerMarshaller(g1WriteMarshaller);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(baos);
        WrappedMessage.writeMessage(ctx, out, g1);
        out.flush();
        baos.flush();
        ctx.unregisterMarshaller(g1WriteMarshaller);

        // read from buffer
        ctx.registerMarshaller(g1ReadMarshaller);
        RawProtoStreamReader in = RawProtoStreamReaderImpl.newInstance(baos.toByteArray());
        G1 result = WrappedMessage.readMessage(ctx, in);
        ctx.unregisterMarshaller(g1ReadMarshaller);

        assertEquals(buildG1(), result);
    }

    private G1 buildG1() {
        G1 g1 = new G1();
        g1.setE1(1);
        g1.setE2("foo");
        g1.setE3(1.234f);
        g1.setE4(null);
        g1.setE5(new String[] {"hello", "world"});
        return g1;
    }

    // this is for sanity debugging while writing the protocol decoder.
    @Test
    public void testMarshallWithComplexNative() throws Exception {
        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ctx.registerProtoFiles(FileDescriptorSource.fromString("tables.proto",
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));
        ctx.registerMarshaller(new G3Marshaller());
        ctx.registerMarshaller(new G4Marshaller());
        ctx.registerMarshaller(new G2Marshaller());

        G2 g2 = buildG2();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(baos);
        WrappedMessage.writeMessage(ctx, out, g2);
        out.flush();
        baos.flush();

        RawProtoStreamReader in = RawProtoStreamReaderImpl.newInstance(baos.toByteArray());
        G2 result = WrappedMessage.readMessage(ctx, in);
        //System.out.println(result);
        assertNotNull(result);
        assertEquals(g2, result);
    }

    private G2 buildG2() {
        G3 g3 = new G3();
        g3.setE1(1);
        g3.setE2("bar");

        G4 g41 = new G4();
        g41.setE1(1);
        g41.setE2("hello");

        G4 g42 = new G4();
        g42.setE1(2);
        g42.setE2("world");

        G2 g2 = new G2();
        g2.setE1(1);
        g2.setE2("foo");
        g2.setG3(g3);
        g2.setG4(Arrays.asList(g41, g42));
        g2.setE5("Hello Infinispan".getBytes());
        g2.setE6(new Timestamp(1489835322801L));
        return g2;
    }

    private InfinispanDocument buildG2(IckleConversionVisitor visitor) throws TranslatorException {
        TreeMap<Integer, TableWireFormat> wireMap = MarshallerBuilder.getWireMap(visitor.getParentTable(),
                visitor.getMetadata());

        InfinispanDocument g2 = new InfinispanDocument("pm1.G2", wireMap, null);
        g2.addProperty("e1", 1);
        g2.addProperty("e2", "foo");

        InfinispanDocument g3 = new InfinispanDocument("pm1.G3", getWireMap(wireMap, "pm1.G3"), g2);
        g3.addProperty("e1", 1);
        g3.addProperty("e2", "bar");
        g2.addChildDocument("pm1.G3", g3);

        InfinispanDocument g41 = new InfinispanDocument("pm1.G4", getWireMap(wireMap, "pm1.G4"), g2);
        g41.addProperty("e1", 1);
        g41.addProperty("e2", "hello");
        g2.addChildDocument("pm1.G4", g41);

        InfinispanDocument g42 = new InfinispanDocument("pm1.G4", getWireMap(wireMap, "pm1.G4"), g2);
        g42.addProperty("e1", 2);
        g42.addProperty("e2", "world");
        g2.addChildDocument("pm1.G4", g42);

        g2.addProperty("e5", "Hello Infinispan");
        g2.addProperty("e6", new Timestamp(1489835322801L));
        return g2;
    }

    private void assertG2(InfinispanDocument result) {
        List<Map<String, Object>> rows = result.flatten();

        assertEquals(2, rows.size());

        Map<String, Object> row = rows.get(0);
        assertEquals(1, row.get("e1"));
        assertEquals("foo", row.get("e2"));
        assertEquals("Hello Infinispan", new String((byte[])row.get("e5")));
        assertEquals(1489835322801L, row.get("e6"));
        assertEquals(1, row.get("pm1.G2/pm1.G3/e1"));
        assertEquals("bar", row.get("pm1.G2/pm1.G3/e2"));
        assertEquals(1, row.get("pm1.G2/pm1.G4/e1"));
        assertEquals("hello", row.get("pm1.G2/pm1.G4/e2"));

        row = rows.get(1);
        assertEquals(1, row.get("e1"));
        assertEquals("foo", row.get("e2"));
        assertEquals(1, row.get("pm1.G2/pm1.G3/e1"));
        assertEquals("bar", row.get("pm1.G2/pm1.G3/e2"));
        assertEquals(2, row.get("pm1.G2/pm1.G4/e1"));
        assertEquals("world", row.get("pm1.G2/pm1.G4/e2"));
    }

    private TreeMap<Integer, TableWireFormat> getWireMap(TreeMap<Integer, TableWireFormat> wireMap, String name) {
        for (TableWireFormat twf : wireMap.values()) {
            if (twf.getAttributeName().equals(name)) {
                return twf.getNestedWireMap();
            }
        }
        return null;
    }

    @Test
    public void testReadComplex() throws Exception {
        IckleConversionVisitor visitor = helpExecute("select * from G2");

        TeiidTableMarsheller readMarshaller = new TeiidTableMarsheller(
                ProtobufMetadataProcessor.getMessageName(visitor.getParentTable()),
                MarshallerBuilder.getWireMap(visitor.getParentTable(), visitor.getMetadata()));

        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ctx.registerProtoFiles(FileDescriptorSource.fromString("tables.proto",
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));

        ctx.registerMarshaller(new G3Marshaller());
        ctx.registerMarshaller(new G4Marshaller());

        G2Marshaller writeMarshaller = new G2Marshaller() {
            @Override
            public G2 readFrom(ProtoStreamReader reader) throws IOException {
                throw new RuntimeException("Use Teiid marshaller for reading for this test..");
            }
        };

        G2 g2 = buildG2();

        // this is used for writing, if reading is being attempted then fail
        ctx.registerMarshaller(writeMarshaller);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(baos);
        WrappedMessage.writeMessage(ctx, out, g2);
        out.flush();
        baos.flush();
        ctx.unregisterMarshaller(writeMarshaller);

        ctx.registerMarshaller(readMarshaller);
        RawProtoStreamReader in = RawProtoStreamReaderImpl.newInstance(baos.toByteArray());
        InfinispanDocument result = WrappedMessage.readMessage(ctx, in);
        //System.out.println(result.flatten());
        assertG2(result);
        ctx.unregisterMarshaller(readMarshaller);
    }



    @Test
    public void testWriteComplex() throws Exception {
        IckleConversionVisitor visitor = helpExecute("select * from G2");

        TeiidTableMarsheller writeMarshaller = new TeiidTableMarsheller(
                ProtobufMetadataProcessor.getMessageName(visitor.getParentTable()),
                MarshallerBuilder.getWireMap(visitor.getParentTable(), visitor.getMetadata()));

        SerializationContext ctx = ProtobufUtil.newSerializationContext(Configuration.builder().build());
        ctx.registerProtoFiles(FileDescriptorSource.fromString("tables.proto",
                ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.proto"))));
        ctx.registerMarshaller(new G3Marshaller());
        ctx.registerMarshaller(new G4Marshaller());

        G2Marshaller readMarshaller = new G2Marshaller() {
            @Override
            public void writeTo(ProtoStreamWriter writer, G2 g2) throws IOException {
                throw new RuntimeException("Use Teiid marshaller for writing for this test..");
            }
        };

        InfinispanDocument g2 = buildG2(visitor);

        ctx.registerMarshaller(writeMarshaller);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RawProtoStreamWriter out = RawProtoStreamWriterImpl.newInstance(baos);
        WrappedMessage.writeMessage(ctx, out, g2);
        out.flush();
        baos.flush();
        ctx.unregisterMarshaller(writeMarshaller);

        // this is used for writing, if reading is being attempted then fail
        ctx.registerMarshaller(readMarshaller);
        RawProtoStreamReader in = RawProtoStreamReaderImpl.newInstance(baos.toByteArray());
        G2 result = WrappedMessage.readMessage(ctx, in);
        ctx.unregisterMarshaller(readMarshaller);
        assertEquals(buildG2(), result);
    }
}
