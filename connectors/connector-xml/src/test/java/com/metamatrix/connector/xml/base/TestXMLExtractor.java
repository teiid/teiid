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

package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import junit.framework.TestCase;

import org.jdom.Document;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;

import com.metamatrix.cdk.api.SysLogger;

public class TestXMLExtractor extends TestCase {
	
	
//	removing hansel while testing clover
/*
	public static Test suite() {
		return new CoverageDecorator(XMLExtractorTest.class, new Class[] {XMLExtractor.class}); 
	}
*/	

	public void testXMLFileExtractor() {
		XMLExtractor extractor = new XMLExtractor(0, true, true, null, new SysLogger(false));
		assertNotNull(extractor);
	}

	// Don't need this any more
//	public void testQuerySourceUninitedFile()  {		
//		String fileName = "";
//		File theFile = new File(fileName);
//		XMLFileExtractor extractor = new XMLFileExtractor();
//		try {
//			extractor.querySource(theFile);
//			//this should fail
//			fail("cannot read from an unspecified file");
//		} catch (IOException ioe) {
//			assertNotNull(ioe);
//		}
//	}
//	
//	public void testQuerySource() {
//		String fileName = ProxyObjectFactory.getDocumentsFolder() + "/state_college.xml";
//		File theFile = new File(fileName);
//		XMLFileExtractor extractor = new XMLFileExtractor();
//		try {
//			String resp = extractor.querySource(theFile);
//			assertNotNull(resp);
//		} catch (IOException ioe) {
//			ioe.printStackTrace();
//			fail(ioe.getMessage());
//		}		
//	}

    public void testCreateDocumentFromString() {
        String file = "<root><foo type='myfoo'/></root>";
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new SysLogger(false));
        try {
            byte[] bytes = file.getBytes();
            InputStream inputStream = new ByteArrayInputStream(bytes);
            Document doc = extractor.createDocumentFromStream(inputStream, "", new NoExtendedFilters()).m_domDoc;
            assertNotNull(doc);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
	

    public void testCreateDocumentFromSlightlyLargerString() {
        StringBuffer buf = new StringBuffer();
        buf.append("<root><foo type='myfoo'/>");
        for (int i = 0; i < 1000; i++) { buf.append("This is some text.\n");}
        buf.append("</root>");
        String file = buf.toString();
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new SysLogger(false));
        try {
            byte[] bytes = file.getBytes();
            InputStream inputStream = new ByteArrayInputStream(bytes);
            Document doc = extractor.createDocumentFromStream(inputStream, "", new NoExtendedFilters()).m_domDoc;
            assertNotNull(doc);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
    
	public void testCreateDocumentFromStringNotXML() {
		//malformed xml
		String file = "<root><foo type='myfoo'/>";
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new SysLogger(false));
		try {			
			byte[] bytes = file.getBytes();
			InputStream inputStream = new ByteArrayInputStream(bytes);
            Document doc = extractor.createDocumentFromStream(inputStream, "", new NoExtendedFilters()).m_domDoc;
			fail("cannot build document from bad xml");
		} catch (ConnectorException e) { 
			assertNotNull(e);
		}		
	}
}
