/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
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
