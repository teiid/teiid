/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.infinispan.hotrod;

import static org.junit.Assert.*;

import java.io.FileReader;
import java.util.Properties;

import org.infinispan.commons.api.BasicCache;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.parser.QueryParser;

public class TestSchemaToProtobufProcessor {

    @SuppressWarnings("rawtypes")
    @Test
    public void testConverstion() throws Exception {
        SchemaToProtobufProcessor tool = new SchemaToProtobufProcessor();
        //tool.setIndexMessages(true);
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        InfinispanConnection conn = Mockito.mock(InfinispanConnection.class);
        BasicCache cache = Mockito.mock(BasicCache.class);
        Mockito.stub(cache.getName()).toReturn("default");
        Mockito.stub(conn.getCache()).toReturn(cache);
        ProtobufResource resource = tool.process(mf, conn);

        String expected = "package model;\n" +
                "\n" +
                "/* @Indexed @Cache(name=foo) */\n" +
                "message G1 {\n" +
                "    /* @Id @Field(index=Index.YES, store=Store.NO) */\n" +
                "    required int32 e1 = 1;\n" +
                "    /* @Field */\n" +
                "    required string e2 = 2;\n" +
                "    optional float e3 = 3;\n" +
                "    /* @Field(index=Index.YES, store=Store.NO) */\n" +
                "    repeated string e4 = 4;\n" +
                "    repeated string e5 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed @Cache(name=default) */\n" +
                "message G2 {\n" +
                "    /* @Id */\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional G3 g3 = 5;\n" +
                "    /* @Field(index=Index.NO) */\n" +
                "    optional bytes e5 = 7;\n" +
                "    /* @Field(index=Index.NO) */\n" +
                "    optional fixed64 e6 = 8;\n" +
                "    repeated G4 g4 = 6;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n" +
                "message G4 {\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional int32 e1 = 3;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed @Cache(name=default) */\n" +
                "message G5 {\n" +
                "    /* @Id */\n"+
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional double e3 = 3;\n" +
                "    optional float e4 = 4;\n" +
                "    /* @Teiid(type=short) */\n" +
                "    optional int32 e5 = 5;\n" +
                "    /* @Teiid(type=byte) */\n" +
                "    optional int32 e6 = 6;\n" +
                "    /* @Teiid(type=char, length=1) */\n" +
                "    optional string e7 = 7;\n" +
                "    optional int64 e8 = 8;\n" +
                "    /* @Teiid(type=bigdecimal) */\n" +
                "    optional string e9 = 9;\n" +
                "    /* @Teiid(type=biginteger) */\n" +
                "    optional string e10 = 10;\n" +
                "    /* @Teiid(type=time) */\n" +
                "    optional int64 e11 = 11;\n" +
                "    /* @Teiid(type=timestamp) */\n" +
                "    optional int64 e12 = 12;\n" +
                "    /* @Teiid(type=date) */\n" +
                "    optional int64 e13 = 13;\n" +
                "    /* @Teiid(type=object) */\n" +
                "    optional bytes e14 = 14;\n" +
                "    /* @Teiid(type=blob) */\n" +
                "    optional bytes e15 = 15;\n" +
                "    /* @Teiid(type=clob) */\n" +
                "    optional bytes e16 = 16;\n" +
                "    /* @Teiid(type=xml) */\n" +
                "    optional bytes e17 = 17;\n" +
                "    /* @Teiid(type=geometry) */\n" +
                "    optional bytes e18 = 18;\n" +
                "}\n" +
                "\n"+
                "message pm1.G3 {\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "}";
        assertEquals(expected, resource.getContents());
    }

    @Test
    public void testSimpleNoMetadataConversion() throws Exception {
        Properties props = new Properties();
        MetadataFactory mf = new MetadataFactory("proto", 1, "model",
                SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        mf.setParser(new QueryParser());
        mf.parse(new FileReader(UnitTestUtil.getTestDataFile("tables_no_metadata.ddl")));

        SchemaToProtobufProcessor tool = new SchemaToProtobufProcessor();
        tool.setIndexMessages(true);
        InfinispanConnection conn = Mockito.mock(InfinispanConnection.class);
        BasicCache cache = Mockito.mock(BasicCache.class);
        Mockito.stub(cache.getName()).toReturn("default");
        Mockito.stub(conn.getCache()).toReturn(cache);
        ProtobufResource resource = tool.process(mf, conn);

        String expected = "package model;\n" +
                "\n" +
                "/* @Indexed */\n"+
                "message G1 {\n" +
                "    /* @Id @Field(index=Index.YES) */\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional float e3 = 3;\n" +
                "    repeated string e4 = 4;\n" +
                "    repeated string e5 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n"+
                "message G2 {\n" +
                "    /* @Id @Field(index=Index.YES) */\n" +
                "    required int32 e1 = 1;\n" +
                "    optional string e2 = 2;\n" +
                "    optional bytes e5 = 3;\n" +
                "    optional int64 e6 = 4;\n" +
                "    repeated G4 g4 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n" +
                "message G4 {\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional double e3 = 3;\n" +
                "    optional float e4 = 4;\n" +
                "    /* @Teiid(type=short) */\n" +
                "    optional int32 e5 = 5;\n" +
                "    /* @Teiid(type=byte) */\n" +
                "    optional int32 e6 = 6;\n" +
                "    /* @Teiid(type=char, length=1) */\n" +
                "    optional string e7 = 7;\n" +
                "    optional int64 e8 = 8;\n" +
                "    /* @Teiid(type=bigdecimal) */\n" +
                "    optional string e9 = 9;\n" +
                "    /* @Teiid(type=biginteger) */\n" +
                "    optional string e10 = 10;\n" +
                "    /* @Teiid(type=time) */\n" +
                "    optional int64 e11 = 11;\n" +
                "    /* @Teiid(type=timestamp) */\n" +
                "    optional int64 e12 = 12;\n" +
                "    /* @Teiid(type=date) */\n" +
                "    optional int64 e13 = 13;\n" +
                "    /* @Teiid(type=object) */\n" +
                "    optional bytes e14 = 14;\n" +
                "    /* @Teiid(type=blob) */\n" +
                "    optional bytes e15 = 15;\n" +
                "    /* @Teiid(type=clob) */\n" +
                "    optional bytes e16 = 16;\n" +
                "    /* @Teiid(type=xml) */\n" +
                "    optional bytes e17 = 17;\n" +
                "    /* @Teiid(type=geometry) */\n" +
                "    optional bytes e18 = 18;\n" +
                "}\n\n";
        assertEquals(expected, resource.getContents());

    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testConverstionUsingCacheAnnotation() throws Exception {
        SchemaToProtobufProcessor tool = new SchemaToProtobufProcessor();
        tool.setIndexMessages(true);
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables_bad.proto");
        InfinispanConnection conn = Mockito.mock(InfinispanConnection.class);
        BasicCache cache = Mockito.mock(BasicCache.class);
        Mockito.stub(cache.getName()).toReturn("foo");
        Mockito.stub(conn.getCache()).toReturn(cache);
        ProtobufResource resource = tool.process(mf, conn);

        String expected = "package model;\n" +
                "\n" +
                "/* @Indexed @Cache(name=foo) */\n" +
                "message G1 {\n" +
                "    /* @Id @Field(index=Index.YES, store=Store.NO) */\n" +
                "    required int32 e1 = 1;\n" +
                "    /* @Field */\n" +
                "    required string e2 = 2;\n" +
                "    optional float e3 = 3;\n" +
                "    /* @Field(index=Index.YES, store=Store.NO) */\n" +
                "    repeated string e4 = 4;\n" +
                "    repeated string e5 = 5;\n" +
                "}\n\n";
        assertEquals(expected, resource.getContents());
    }

}
