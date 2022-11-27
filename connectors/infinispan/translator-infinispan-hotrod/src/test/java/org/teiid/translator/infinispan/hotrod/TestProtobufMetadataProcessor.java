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

import java.util.Properties;
import java.util.TreeMap;

import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.infinispan.api.MarshallerBuilder;
import org.teiid.infinispan.api.ProtobufMetadataProcessor;
import org.teiid.infinispan.api.TableWireFormat;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

public class TestProtobufMetadataProcessor {

    public static TransformationMetadata getTransformationMetadata(MetadataFactory mf, InfinispanExecutionFactory ef)
            throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(),
                "proto", new FunctionTree("foo", new UDFSource(ef.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(),
                metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        return metadata;
    }

    public static MetadataFactory protoMatadata(String protoFile) throws TranslatorException {
        return protoMatadata(protoFile, false);
    }

    public static MetadataFactory protoMatadata(String protoFile, boolean useClasspath) throws TranslatorException {
        ProtobufMetadataProcessor processor = new ProtobufMetadataProcessor();
        if (useClasspath) {
            processor.setProtoFilePath(protoFile);
        } else {
            processor.setProtoFilePath(UnitTestUtil.getTestDataPath() + "/"+protoFile);
        }

        Properties props = new Properties();
        MetadataFactory mf = new MetadataFactory("proto", 1, "model",
                SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        processor.process(mf, null);
        return mf;
    }

    @Test
    public void testMetadataProcessor() throws Exception {
        MetadataFactory mf = protoMatadata("tables.proto", false);
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //ObjectConverterUtil.write(new StringReader(ddl), UnitTestUtil.getTestDataFile("tables.ddl"));
        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.ddl")), ddl);
    }

    @Test
    public void testMetadataProcessorClasspath() throws Exception {
        MetadataFactory mf = protoMatadata("tables.proto", true);
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //ObjectConverterUtil.write(new StringReader(ddl), UnitTestUtil.getTestDataFile("tables.ddl"));
        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.ddl")), ddl);
    }

    @Test
    public void testMetadataProcessorAddressbook() throws Exception {
        MetadataFactory mf = protoMatadata("addressbook.proto");
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);
        //ObjectConverterUtil.write(new StringReader(ddl), UnitTestUtil.getTestDataFile("tables.ddl"));
        //assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("tables.ddl")), ddl);
    }

    @Test
    public void testTableWireFormat() throws Exception {
        MetadataFactory mf = protoMatadata("tables.proto");
        InfinispanExecutionFactory ef = new InfinispanExecutionFactory();
        TransformationMetadata metadata = getTransformationMetadata(mf, ef);

        TreeMap<Integer, TableWireFormat> map = MarshallerBuilder.getWireMap(mf.getSchema().getTable("G2"),
                new RuntimeMetadataImpl(metadata));
        String expected = "{8=TableWireFormat [expectedTag=8, attributeName=e1, nested=null], "
                + "18=TableWireFormat [expectedTag=18, attributeName=e2, nested=null], "
                + "42=TableWireFormat [expectedTag=42, attributeName=pm1.G3, nested={"
                    + "8=TableWireFormat [expectedTag=8, attributeName=pm1.G2/pm1.G3/e1, nested=null], "
                    + "18=TableWireFormat [expectedTag=18, attributeName=pm1.G2/pm1.G3/e2, nested=null]}], "
                + "50=TableWireFormat [expectedTag=50, attributeName=pm1.G4, nested={"
                    + "8=TableWireFormat [expectedTag=8, attributeName=pm1.G2/pm1.G4/e1, nested=null], "
                    + "18=TableWireFormat [expectedTag=18, attributeName=pm1.G2/pm1.G4/e2, nested=null]}], "
                    + "58=TableWireFormat [expectedTag=58, attributeName=e5, nested=null], "
                + "65=TableWireFormat [expectedTag=65, attributeName=e6, nested=null]}";

        assertEquals(expected, map.toString());
    }
}