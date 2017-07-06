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

import java.io.FileReader;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.infinispan.api.InfinispanConnection;
import org.teiid.infinispan.api.ProtobufResource;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.parser.QueryParser;
import static org.junit.Assert.*;

public class TestSchemaToProtobufProcessor {

    @Test
    public void testConverstion() throws Exception {
        SchemaToProtobufProcessor tool = new SchemaToProtobufProcessor();
        MetadataFactory mf = TestProtobufMetadataProcessor.protoMatadata("tables.proto");
        ProtobufResource resource = tool.process(mf, Mockito.mock(InfinispanConnection.class));

        String expected = "package model;\n" +
                "\n" +
                "/* @Indexed */\n" +
                "message G1 {\n" +
                "    /* @Id @IndexedField(index=true, store=false) */\n" +
                "    required int32 e1 = 1;\n" +
                "    /* @IndexedField */\n" +
                "    required string e2 = 2;\n" +
                "    optional float e3 = 3;\n" +
                "    /* @IndexedField(index=true, store=false) */\n" +
                "    repeated string e4 = 4;\n" +
                "    repeated string e5 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n" +
                "message G2 {\n" +
                "    /* @Id */\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional G3 g3 = 5;\n" +
                "    /* @IndexedField(index=false) */\n" +
                "    optional bytes e5 = 7;\n" +
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
        ProtobufResource resource = tool.process(mf, Mockito.mock(InfinispanConnection.class));

        String expected = "package model;\n" +
                "\n" +
                "/* @Indexed */\n"+
                "message G1 {\n" +
                "    /* @Id */\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "    optional float e3 = 3;\n" +
                "    repeated string e4 = 4;\n" +
                "    repeated string e5 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n"+
                "message G2 {\n" +
                "    /* @Id */\n" +
                "    required int32 e1 = 1;\n" +
                "    optional string e2 = 2;\n" +
                "    optional bytes e5 = 3;\n" +
                "    optional int64 e6 = 4;\n" +
                "    repeated G4 g4 = 5;\n" +
                "}\n" +
                "\n" +
                "/* @Indexed */\n"+
                "message G4 {\n" +
                "    required int32 e1 = 1;\n" +
                "    required string e2 = 2;\n" +
                "}\n\n";
        assertEquals(expected, resource.getContents());

    }
}
