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

package com.metamatrix.internal.core.xml.xmi;

import java.io.File;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * XMIHeaderReaderTest
 */
public class TestXMIHeaderReader extends TestCase {

    /**
     * Constructor for TestMetadataLoadingCache.
     * @param name
     */
    public TestXMIHeaderReader(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================


    private XMIHeader helpReadHeader( final XMIHeaderReader reader, final File f, final boolean expectSuccess ) {
        assertNotNull(reader);
        assertNotNull(f);
        XMIHeader header  = null;
        try {
            header = reader.read(f);
        } catch (Throwable e) {
            if (expectSuccess) {
            	throw new RuntimeException(e);
            }
        }
        if (!expectSuccess && header != null) {
            fail("Expected failure but instead found header " + header); //$NON-NLS-1$
        }
        return header;
    }

    public void testRead() {
        XMIHeaderReader reader = new XMIHeaderReader();
        File testFile = null;
        XMIHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("nonEmptyModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(5,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:6613f180-10ab-1ecc-b71a-a65bb31ca97b",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("emptyModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(3,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:7fd3f8c0-0ff4-1ecc-bb21-bf6f5277bd68",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("nonModelFile.txt"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("sampleMSWord.doc"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("BooksSQLServer.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(6,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:3ba8ad00-1272-1ed1-b625-afaffdadee3e",header.getUUID()); //$NON-NLS-1$
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());

        testFile = UnitTestUtil.getTestDataFile("BooksCatalogDoc.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());
        assertEquals(7,header.getNamespaceURIs().length);
        assertEquals(4,header.getModelImportInfos().length);
        assertEquals("mmuuid:cf338c40-fa5e-1f0e-8827-b258024845e3",header.getUUID()); //$NON-NLS-1$
        ModelImportInfo info = header.getModelImportInfos()[1];
        assertEquals("BooksOracle",info.getName()); //$NON-NLS-1$
        assertEquals("/TestProject0520/BooksOracle.xmi",info.getPath()); //$NON-NLS-1$
        assertEquals("mmuuid:ffa4c2c0-f961-1f0e-8827-b258024845e3",info.getUUID()); //$NON-NLS-1$
        assertEquals("PHYSICAL",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/metamodels/Relational",info.getPrimaryMetamodelURI()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("virtualModelWithImports.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());
        assertEquals(7,header.getNamespaceURIs().length);
        assertEquals(2,header.getModelImportInfos().length);
        assertEquals("mmuuid:e30f25c0-13d6-1ed4-852d-b1622e809d4f",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("Books.xsd"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(3,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getNamespaceURIs().length);
        assertEquals(2,header.getModelImportInfos().length);
        assertEquals("mmuuid:738adb40-21b2-1edb-bb0f-84717d38b299",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel2.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(3,header.getNamespaceURIs().length);
        assertEquals(5,header.getModelImportInfos().length);
        assertEquals("mmuuid:2f21ec00-138c-1f2d-a72f-ff6ac3dbb7c0",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel3.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(3,header.getNamespaceURIs().length);
        assertEquals(9,header.getModelImportInfos().length);
        assertEquals("mmuuid:d4ee4e00-cffe-1ef2-9d34-e6f134eadf81",header.getUUID()); //$NON-NLS-1$

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel4.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:698cda02-5479-1f93-8b1e-9e2dfc4e3733",header.getUUID()); //$NON-NLS-1$

        // A MetaMatrix version 4.0(+) initial model with one root
        testFile = UnitTestUtil.getTestDataFile("SingleRootEmpty.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:e103e100-47a7-1f04-9a10-e53b219c8f76",header.getUUID()); //$NON-NLS-1$
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());

        // A MetaMatrix version 2.0 model file
        testFile = UnitTestUtil.getTestDataFile("partsSupplierOracle_v0200.xml"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("1.1",header.getXmiVersion()); //$NON-NLS-1$
        assertNull(header.getUUID());
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());

        // A MetaMatrix version 3.0 model file
        testFile = UnitTestUtil.getTestDataFile("VirtualNorthwind.xml"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("1.1",header.getXmiVersion()); //$NON-NLS-1$
        assertNull(header.getUUID());
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());

        // A MetaMatrix version 4.1 Uml model file
        testFile = UnitTestUtil.getTestDataFile("TestUml4.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getNamespaceURIs().length);
        assertEquals(0,header.getModelImportInfos().length);
        assertEquals("mmuuid:98783900-c3e0-1f07-9a25-c5dc8f8cd8d9",header.getUUID()); //$NON-NLS-1$
        assertNull(header.getProducerName());
        assertNull(header.getProducerVersion());

        // A MetaMatrix version 5.0 model file
        testFile = UnitTestUtil.getTestDataFile("BQT_SQLServer.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getModelImportInfos().length);
        assertEquals("mmuuid:99a9be00-e527-1ff0-b108-85ef0ae7b7f4",header.getUUID()); //$NON-NLS-1$
        assertEquals("MetaMatrix",header.getProducerName()); //$NON-NLS-1$
        assertEquals("5.5",header.getProducerVersion()); //$NON-NLS-1$

    }
}
