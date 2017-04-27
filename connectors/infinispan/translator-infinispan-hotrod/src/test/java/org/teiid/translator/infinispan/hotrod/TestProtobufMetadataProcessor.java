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

import static org.junit.Assert.assertEquals;

import java.util.Properties;
import java.util.TreeMap;

import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
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
        ProtobufMetadataProcessor processor = new ProtobufMetadataProcessor();
        processor.setProtoFilePath(UnitTestUtil.getTestDataPath() + "/"+protoFile);

        Properties props = new Properties();
        MetadataFactory mf = new MetadataFactory("proto", 1, "model",
                SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        processor.process(mf, null);
        return mf;
    }

    @Test
    public void testMetadataProcessor() throws Exception {
        MetadataFactory mf = protoMatadata("tables.proto");
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);
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