package com.metamatrix.connector.xml;

import junit.framework.TestCase;

import org.xml.sax.XMLReader;

import com.metamatrix.connector.xml.streaming.ReaderFactory;

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
