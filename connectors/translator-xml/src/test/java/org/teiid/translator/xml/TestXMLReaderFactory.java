package org.teiid.translator.xml;

import junit.framework.TestCase;

import org.teiid.translator.xml.streaming.ReaderFactory;
import org.xml.sax.XMLReader;


public class TestXMLReaderFactory extends TestCase {

	public void testGetSAXBuilder() {
		try {
			XMLReader reader = ReaderFactory.getXMLReader(null);
			assertNotNull(reader);
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}
}
