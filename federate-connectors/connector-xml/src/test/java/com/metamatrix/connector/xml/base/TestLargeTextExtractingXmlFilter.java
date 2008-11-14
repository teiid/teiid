/*
 * © 2007 Varsity Gateway LLC. All Rights Reserved.
 */

package com.metamatrix.connector.xml.base;

import java.io.ByteArrayInputStream;
import java.io.File;

import junit.framework.TestCase;

import org.jdom.Document;
import org.jdom.JDOMException;
import org.jdom.input.SAXBuilder;
import org.xml.sax.XMLFilter;
import org.xml.sax.XMLReader;

import com.metamatrix.data.api.ConnectorLogger;

public class TestLargeTextExtractingXmlFilter extends TestCase {
	
	private static final String strCache = "cache";
	private File cache = new File(strCache);

	public TestLargeTextExtractingXmlFilter(String arg0) {
		super(arg0);
	}
	
	public void setUp() {
		if(!cache.exists()) {
			cache.mkdir();
		}
	}
	
	public void tearDown() {
		if(cache.exists()) {
			cache.delete();
		}
	}


	public void testCharacters() {
		int maxInMemoryStringSize = 10;
		ConnectorLogger logger = new XMLConnector().getLogger();
		
		char[] array = new char[512];
		for(int i =0; i < array.length; i++) {
			array[i] = 'd';			
		}
		StringBuffer doc = new StringBuffer();
		doc.append("<doc><payload>");
		for(int j = 0; j < 100; j++) {
			doc.append(array);	
		}
		doc.append("</payload></doc>");
		
		ByteArrayInputStream responseBody = new ByteArrayInputStream(doc.toString().getBytes());


        CountingInputStream stream = new CountingInputStream(responseBody);
        SAXBuilder builder;

        LargeTextExtractingXmlFilter filter = null;
        filter = new LargeTextExtractingXmlFilter(maxInMemoryStringSize, cache, logger);

        try {
        	assertTrue(stream.available() > 0);
        
            // See SAXBuilderFix for why we need it instead of plain SAXBuilder
        	builder = new SAXBuilderFix(filter);
        	builder.setXMLFilter(filter);

        	Document domDoc = builder.build(stream);
        	assertNotNull(domDoc);
        } catch (Exception se) {
        	se.printStackTrace();
        	fail(se.getMessage());
        }
            
	}

	
	public void testCharactersWithSpaces() {
		int maxInMemoryStringSize = 10;
		ConnectorLogger logger = new XMLConnector().getLogger();
		
		char[] array = new char[512];
		for(int i =0; i < array.length; i++) {
			//insert spaces every 100th element
			if(i % 100 == 0) {
				array[i] = ' ';
			} else {
				array[i] = 'd';
			}
		}
		StringBuffer doc = new StringBuffer();
		doc.append("<doc><payload><metadata>bigstring</metadata>");
		for(int j = 0; j < 100; j++) {
			doc.append(array);	
		}
		doc.append("</payload></doc>");
		
		ByteArrayInputStream responseBody = new ByteArrayInputStream(doc.toString().getBytes());


        CountingInputStream stream = new CountingInputStream(responseBody);
        SAXBuilder builder;

        LargeTextExtractingXmlFilter filter = null;
        filter = new LargeTextExtractingXmlFilter(maxInMemoryStringSize, cache, logger);

        try {
        	assertTrue(stream.available() > 0);
        
            // See SAXBuilderFix for why we need it instead of plain SAXBuilder
        	builder = new SAXBuilderFix(filter);
        	builder.setXMLFilter(filter);

        	Document domDoc = builder.build(stream);
        	assertNotNull(domDoc);
        } catch (Exception se) {
        	se.printStackTrace();
        	fail(se.getMessage());
        }
            
	}
	
	public void testIgnorableWhitespace() {

	}


	public void testLargeTextExtractingXmlFilter() {

	}


	public void testStartElementStringStringStringAttributes() {

	}


	public void testEndElementStringStringString() {

	}

	
	public void testString() {

	}


	public void testElement() {

	}


	public void testGetXmlStringForFile() {

	}


	public void testStringOrValueReference() {

	}


	public void testGetFiles() {

	}
	
	//**************************************************************************************************
	//TODO: This class is private to XMLExtractor so be sure it is kept syncronized with what is there
	//It should be fairly static
	
    // This class is a bug fix override for SAXBuilder. SAXBuilder
    // fails to properly insert the filter in the chain
    private final class SAXBuilderFix extends SAXBuilder {
        private final XMLFilter filter;

        private SAXBuilderFix(XMLFilter filter) {
            super();
            this.filter = filter;
        }

        protected XMLReader createParser() throws JDOMException {
            // SAXBuilder fails to properly insert the filter into the
            // chain. We know exactly what the filter is (this.filter),
            // so we can insert it ourselves directly.
            XMLReader parser = super.createParser();
            XMLFilter root = filter;
            root.setParent(parser);
            parser = filter;
            return parser;
        }
        
        // Since SAXBuilder fails to properly insert the filter into the
        // chain, we need to swallow this method. createParser knows about
        // the filter through other means.
        public void setXMLFilter(XMLFilter xmlFilter)
        {
            
        }

    }

}
