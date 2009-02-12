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

package com.metamatrix.internal.core.xml.vdb;

import java.io.File;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * XMIHeaderReaderTest
 */
public class TestVdbHeaderReader extends TestCase {

    // -------------------------------------------------
    // Variables initialized during one-time startup ...
    // -------------------------------------------------

    // ---------------------------------------
    // Variables initialized for each test ...
    // ---------------------------------------

    // =========================================================================
    //                        F R A M E W O R K
    // =========================================================================

    /**
     * Constructor for TestVdbHeaderReader.
     * @param name
     */
    public TestVdbHeaderReader(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================


    private VdbHeader helpReadHeader( final VdbHeaderReader reader, final File f, final boolean expectSuccess ) {
        assertNotNull(reader);
        assertNotNull(f);
        VdbHeader header  = null;
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

    public void testCreate() {
        new VdbHeaderReader();
    }

    public void testReadNonEmptyModel() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("nonEmptyModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertNull(header.getUUID());
    }

    public void testReadEmptyModel() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("emptyModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertNull(header.getUUID());
    }

    public void testReadNonModelFile() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("nonModelFile.txt"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);
    }

    public void testReadSampleMSWordDoc() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("sampleMSWord.doc"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);
    }

    public void testReadManifest() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(2,header.getModelInfos().length);
        assertEquals(0,header.getNonModelInfos().length);
        assertEquals("mmuuid:738adb40-21b2-1edb-bb0f-84717d38b299",header.getUUID()); //$NON-NLS-1$
        assertEquals(VdbHeader.SEVERITY_ERROR,header.getSeverity());
        assertEquals(3,header.getSeverityCode());
    }

    public void testReadManifest2() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel2.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(5,header.getModelInfos().length);
        assertEquals(0,header.getNonModelInfos().length);
        assertEquals("mmuuid:2f21ec00-138c-1f2d-a72f-ff6ac3dbb7c0",header.getUUID()); //$NON-NLS-1$
        assertTrue(header.getDescription().startsWith("More than a million people along Florida's east")); //$NON-NLS-1$
        assertEquals(VdbHeader.SEVERITY_OK,header.getSeverity());
        assertEquals(0,header.getSeverityCode());
        assertEquals("MetaMatrix",header.getProducerName()); //$NON-NLS-1$
        assertEquals("4.2.0",header.getProducerVersion()); //$NON-NLS-1$
        assertEquals("2004-09-02T13:24:29.872-06:00",header.getTimeLastChanged()); //$NON-NLS-1$
        assertEquals("2004-09-02T13:24:29.872-06:00",header.getTimeLastProduced()); //$NON-NLS-1$

        VdbModelInfo info = header.getModelInfos()[0];
        assertEquals("PartsSupplierOracle.xmi",info.getName()); //$NON-NLS-1$
        assertEquals("/MaterializedViewTesting/PartsSupplierOracle.xmi",info.getPath()); //$NON-NLS-1$
        assertEquals("mmuuid:a2527680-1360-1f2d-a72f-ff6ac3dbb7c0",info.getUUID()); //$NON-NLS-1$
        assertEquals("PHYSICAL",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/metamodels/Relational",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(0,info.getCheckSum());
    }

    public void testReadManifest3() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel3.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(9,header.getModelInfos().length);
        assertEquals(0,header.getNonModelInfos().length);
        assertEquals("mmuuid:d4ee4e00-cffe-1ef2-9d34-e6f134eadf81",header.getUUID()); //$NON-NLS-1$
        assertEquals("MetaMatrix",header.getProducerName()); //$NON-NLS-1$
        assertEquals("Apollo",header.getProducerVersion()); //$NON-NLS-1$
        assertEquals(VdbHeader.SEVERITY_WARNING,header.getSeverity());
        assertEquals(2,header.getSeverityCode());
    }

    public void testReadManifest4() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel4.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(0,header.getModelInfos().length);
        assertEquals(1,header.getNonModelInfos().length);
        assertEquals("mmuuid:698cda02-5479-1f93-8b1e-9e2dfc4e3733",header.getUUID()); //$NON-NLS-1$

        VdbNonModelInfo info2 = header.getNonModelInfos()[0];
        assertEquals("SampleRelational.xmi",info2.getName()); //$NON-NLS-1$
        assertEquals("/ProjectForSimpleVdb/SampleRelational.xmi",info2.getPath()); //$NON-NLS-1$
        assertEquals(2645090403L,info2.getCheckSum());
    }

    public void testReadManifest5() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel5.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(7,header.getModelInfos().length);
        assertEquals(0,header.getNonModelInfos().length);
        assertEquals("mmuuid:6d179c02-f639-1f59-a112-e0eac778cc3c",header.getUUID()); //$NON-NLS-1$
        assertTrue(header.getDescription().startsWith("This virtual database (VDB) definition file contains the models")); //$NON-NLS-1$
        assertEquals(VdbHeader.SEVERITY_WARNING,header.getSeverity());
        assertEquals(2,header.getSeverityCode());
        assertEquals("MetaMatrix",header.getProducerName()); //$NON-NLS-1$
        assertEquals("4.2.0",header.getProducerVersion()); //$NON-NLS-1$
        assertEquals("2005-08-02T11:32:38.375-06:00",header.getTimeLastChanged()); //$NON-NLS-1$
        assertEquals("2005-08-02T11:32:38.375-06:00",header.getTimeLastProduced()); //$NON-NLS-1$

        VdbModelInfo info = header.getModelInfos()[3];
        assertEquals("BooksXML.xmi",info.getName()); //$NON-NLS-1$
        assertEquals("/Books Project/BooksXML.xmi",info.getPath()); //$NON-NLS-1$
        assertEquals("mmuuid:f96d8dc0-0dc9-1eec-8518-c32201e76066",info.getUUID()); //$NON-NLS-1$
        assertEquals("VIRTUAL",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/metamodels/XmlDocument",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(4222954943L,info.getCheckSum());

        info = header.getModelInfos()[1];
        assertEquals("Books.xsd",info.getName()); //$NON-NLS-1$
        assertEquals("/Books Project/Books.xsd",info.getPath()); //$NON-NLS-1$
        assertNull(info.getUUID());
        assertEquals("TYPE",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.eclipse.org/xsd/2002/XSD",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(2392354248L,info.getCheckSum());
    }

    public void testReadManifest6() {
        VdbHeaderReader reader = new VdbHeaderReader();
        File testFile = null;
        VdbHeader header = null;

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel6.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertEquals(4,header.getModelInfos().length);
        assertEquals(1,header.getNonModelInfos().length);
        assertEquals("System",header.getName()); //$NON-NLS-1$
        assertEquals("mmuuid:7abbce82-5efe-1f59-bb2d-ec2ea3ba5f60",header.getUUID()); //$NON-NLS-1$
        assertEquals(VdbHeader.SEVERITY_WARNING,header.getSeverity());
        assertEquals(2,header.getSeverityCode());
        assertEquals("MetaMatrix",header.getProducerName()); //$NON-NLS-1$
        assertEquals("4.3.0",header.getProducerVersion()); //$NON-NLS-1$
        assertEquals("2005-08-09T15:25:51.462-06:00",header.getTimeLastChanged()); //$NON-NLS-1$
        assertEquals("2005-08-09T15:25:51.462-06:00",header.getTimeLastProduced()); //$NON-NLS-1$

        VdbModelInfo info = header.getModelInfos()[0];
        assertEquals("SystemSchema.xsd",info.getName()); //$NON-NLS-1$
        assertEquals("/System/SystemSchema.xsd",info.getPath()); //$NON-NLS-1$
        assertNull(info.getUUID());
        assertEquals("TYPE",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.eclipse.org/xsd/2002/XSD",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(3565284344L,info.getCheckSum());

        info = header.getModelInfos()[1];
        assertEquals("SystemDocument.xmi",info.getName()); //$NON-NLS-1$
        assertEquals("/System/SystemDocument.xmi",info.getPath()); //$NON-NLS-1$
        assertEquals("mmuuid:b77b1cc0-dd8d-1ee9-a82c-9f4fb5468132",info.getUUID()); //$NON-NLS-1$
        assertEquals("VIRTUAL",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/metamodels/XmlDocument",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(2957602990L,info.getCheckSum());

        info = header.getModelInfos()[3];
        assertEquals("System.xmi",info.getName()); //$NON-NLS-1$
        assertEquals("/System/System.xmi",info.getPath()); //$NON-NLS-1$
        assertEquals("mmuuid:70ffc880-29d8-1de6-8a38-9d76e1f90f2e",info.getUUID()); //$NON-NLS-1$
        assertEquals("VIRTUAL",info.getModelType()); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/metamodels/Relational",info.getPrimaryMetamodelURI()); //$NON-NLS-1$
        assertEquals(1316247188L,info.getCheckSum());

        VdbNonModelInfo info2 = header.getNonModelInfos()[0];
        assertEquals("System.DEF",info2.getName()); //$NON-NLS-1$
        assertEquals("System.DEF",info2.getPath()); //$NON-NLS-1$
        assertEquals(1667846304L,info2.getCheckSum());
    }
}
