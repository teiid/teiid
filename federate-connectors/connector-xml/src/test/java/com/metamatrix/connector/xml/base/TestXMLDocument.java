/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import junit.framework.TestCase;

import org.jdom.Document;

/**
 * created by JChoate on Jun 27, 2005
 *
 */
public class TestXMLDocument extends TestCase {

    /**
     * Constructor for XMLDocumentTest.
     * @param arg0
     */
    
	
//	removing hansel while testing clover
/*	
	public static Test suite() {
		return new CoverageDecorator(XMLDocumentTest.class, new Class[] {XMLDocument.class}); 
	}
*/	
	
    public TestXMLDocument(String arg0) {
        super(arg0);
    }

    /*
     * Class under test for void XMLDocument()
     */
    public void testXMLDocument() {
        XMLDocument doc = new XMLDocument();
        assertNotNull("XMLDocument is null", doc);
    }

    /*
     * Class under test for void XMLDocument(Document, Hashtable)
     */
    public void testXMLDocumentDocumentHashtable() {
        try {
            Document doc = ProxyObjectFactory.getDefaultDocument();
            XMLDocument xmlDoc = new XMLDocument(doc, new FileLifeManager[]{null});
            assertNotNull(xmlDoc);
            assertEquals(doc, xmlDoc.getContextRoot());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
      
    }

    public void testSetGetDocument() {
        try {
            Document doc = ProxyObjectFactory.getDefaultDocument();
            XMLDocument xmlDoc = new XMLDocument();
            xmlDoc.setContextRoot(doc);
            assertEquals(doc, xmlDoc.getContextRoot());
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

}
