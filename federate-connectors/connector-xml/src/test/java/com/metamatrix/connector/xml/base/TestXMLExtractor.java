/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import junit.framework.TestCase;

import org.jdom.Document;

import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.exception.ConnectorException;

public class TestXMLExtractor extends TestCase {
	
	
//	removing hansel while testing clover
/*
	public static Test suite() {
		return new CoverageDecorator(XMLExtractorTest.class, new Class[] {XMLExtractor.class}); 
	}
*/	

    private static class DummyLogger implements ConnectorLogger
    {
        public void logError(String s) {
        	System.out.println(s);
        }
        
        public void logError(String s, Throwable e) {
            System.out.println(s);
            e.printStackTrace();
        }
        
        public void logWarning(String s) {
            System.out.println(s);
        }
        
        public void logInfo(String s) {
            System.out.println(s);
        }
        
        public void logDetail(String s) {
            System.out.println(s);
        }
        
        public void logTrace(String s) {     
            System.out.println(s);
        }
        
    }
	public void testXMLFileExtractor() {
		XMLExtractor extractor = new XMLExtractor(0, true, true, null, new DummyLogger());
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
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new DummyLogger());
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
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new DummyLogger());
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
        XMLExtractor extractor = new XMLExtractor(0, true, true, null, new DummyLogger());
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
