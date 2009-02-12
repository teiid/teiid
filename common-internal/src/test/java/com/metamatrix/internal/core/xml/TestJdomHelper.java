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

package com.metamatrix.internal.core.xml;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import junit.framework.TestCase;

import org.jdom.Document;
import org.jdom.Element;

import com.metamatrix.core.util.UnitTestUtil;

/**
 * This class tests the functionality in the JdomHelper class
 */
public class TestJdomHelper extends TestCase {
//    private static final String TEST_SCHEMA1   = "xml/Stereo.xsd";
    private static final String TEST_DOCUMENT1 = "testdoc.xml"; //$NON-NLS-1$

    // ################################## FRAMEWORK ################################

    public TestJdomHelper(String name) {
        super(name);
    }

    // ################################## TEST HELPERS ################################

    /**
     */
    private Document helpBuildDocumentFromStream(String filename, boolean validateXML) throws Exception {
        InputStream stream = null;
        Document result = null;
        try {
            stream = new FileInputStream(UnitTestUtil.getTestDataFile(filename));
            result = JdomHelper.buildDocument(stream, validateXML);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                }
            }
        }
        assertNotNull("The Document built from \""+filename+"\" is null",result); //$NON-NLS-1$ //$NON-NLS-2$
        assertNotNull("The Document built from \""+filename+"\" is empty",result.getRootElement()); //$NON-NLS-1$ //$NON-NLS-2$
        return result;
    }

    // ################################## ACTUAL TESTS ################################

    /**
     * Test ...
     */
    public void testBuildFromStream1() throws Exception {
        helpBuildDocumentFromStream(TEST_DOCUMENT1,false);
    }
    
    public void testWritingDocWithEmbeddedDoc() throws Exception {
        // Create a simple doc 
        final Document doc = JdomHelper.createNewDocument("A"); //$NON-NLS-1$
        final Element elementB = new Element("B"); //$NON-NLS-1$
        doc.getRootElement().addContent(elementB);
        final Element elementC = new Element("C"); //$NON-NLS-1$
        elementB.addContent(elementC);
        
        final Document embeddedDoc = helpBuildDocumentFromStream(TEST_DOCUMENT1,false);
        String embeddedDocString = JdomHelper.write(embeddedDoc);

        // Set the text on "C" to be another XML doc
        elementC.setText(embeddedDocString);
        
        String docString = JdomHelper.write(doc);
        assertEquals(728, docString.length());
        
        // Print the doc to standard out ...
        final String defaultIndent = "  "; //$NON-NLS-1$
        final boolean newlines = true;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        JdomHelper.write(doc, baos, defaultIndent, newlines);
        assertEquals(728, baos.size());
    }

}
