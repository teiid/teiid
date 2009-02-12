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

package com.metamatrix.internal.core.xml.xsd;

import java.io.File;
import java.io.FileInputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import junit.framework.TestCase;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * XsdHeaderReaderTest
 */
public class TestXsdHeaderReader extends TestCase {

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
     * Constructor for TestMetadataLoadingCache.
     * @param name
     */
    public TestXsdHeaderReader(String name) {
        super(name);
    }

    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================


    private XsdHeader helpReadHeader( final XsdHeaderReader reader, final File f, final boolean expectSuccess ) {
        assertNotNull(reader);
        assertNotNull(f);
        XsdHeader header  = null;
        try {
            header = reader.read(f);
        } catch (Throwable e) {
            if (expectSuccess) {
                throw new RuntimeException(e);
            }
        }
        if (expectSuccess && header == null) {
            fail("Expected success but the header was null"); //$NON-NLS-1$
        }
        if (!expectSuccess && header != null) {
            fail("Expected failure but instead found header " + header); //$NON-NLS-1$
        }
        return header;
    }

    public void testRead() {
        XsdHeaderReader reader = new XsdHeaderReader();
        File testFile = UnitTestUtil.getTestDataFile("nonEmptyModel.xmi"); //$NON-NLS-1$
        XsdHeader header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("emptyModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("nonModelFile.txt"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("sampleMSWord.doc"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("BooksSQLServer.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("virtualModelWithImports.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("Books.xsd"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,true);
        assertNotNull(header);
        assertEquals("http://www.metamatrix.com/XMLSchema/DataSets/Books",header.getTargetNamespaceURI()); //$NON-NLS-1$
        assertEquals(3,header.getNamespaceURIs().length);
        assertEquals("http://www.w3.org/2001/XMLSchema",header.getNamespaceURIs()[0]); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/XMLSchema/DataSets/Books/BookDatatypes",header.getNamespaceURIs()[1]); //$NON-NLS-1$
        assertEquals("http://www.metamatrix.com/XMLSchema/DataSets/Books",header.getNamespaceURIs()[2]); //$NON-NLS-1$
        assertEquals(1,header.getImportNamespaces().length);
        assertEquals("http://www.metamatrix.com/XMLSchema/DataSets/Books/BookDatatypes",header.getImportNamespaces()[0]); //$NON-NLS-1$
        assertEquals(1,header.getImportSchemaLocations().length);
        assertEquals("BookDatatypes.xsd",header.getImportSchemaLocations()[0]); //$NON-NLS-1$
        assertEquals(0,header.getIncludeSchemaLocations().length);

        testFile = UnitTestUtil.getTestDataFile("nonExistentFile.xsd"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        testFile = UnitTestUtil.getTestDataFile("MetaMatrix-VdbManifestModel.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        // A MetaMatrix version 4.0(+) initial model with one root
        testFile = UnitTestUtil.getTestDataFile("SingleRootEmpty.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        // A MetaMatrix version 2.0 model file
        testFile = UnitTestUtil.getTestDataFile("partsSupplierOracle_v0200.xml"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        // A MetaMatrix version 3.0 model file
        testFile = UnitTestUtil.getTestDataFile("VirtualNorthwind.xml"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);

        // A MetaMatrix version 4.1 Uml model file
        testFile = UnitTestUtil.getTestDataFile("TestUml4.xmi"); //$NON-NLS-1$
        header = helpReadHeader(reader,testFile,false);
        assertNull(header);
    }

    public void testReadException() throws Exception{
        FileInputStream istream = new FileInputStream(UnitTestUtil.getTestDataFile("Books.xsd")); //$NON-NLS-1$
        try {
            DefaultHandler handler = new TerminatingXsdHeaderContentHandler();
            Thread.currentThread().setContextClassLoader(XsdHeaderReader.class.getClassLoader());
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);
            SAXParser parser = spf.newSAXParser();
            parser.parse(new InputSource(istream), handler);
        } catch (SAXException e) {
            //expected
            return;
        }
        fail("Did not get expected exception");//$NON-NLS-1$
    }
}
