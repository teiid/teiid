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

package org.teiid.query.function.source;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.sql.Timestamp;
import java.util.TimeZone;

import javax.xml.xpath.XPathExpressionException;

import org.jdom.Attribute;
import org.jdom.Element;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.FileUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.function.source.XMLSystemFunctions;


@SuppressWarnings("nls")
public class TestXMLSystemFunctions {
	
    public String getContentOfTestFile( final String testFilePath ) {
        final File file = UnitTestUtil.getTestDataFile(testFilePath);
        final FileUtil util = new FileUtil(file.getAbsolutePath());
        return util.read();
    }
    
    public String helpTestXpathValue(final String xmlFilePath, final String xpath, 
                                     String namespaces, final String expected) throws IOException, XPathExpressionException, FunctionExecutionException {
        final String actual = helpGetNode(xmlFilePath,xpath, namespaces);
        assertEquals(expected,actual);
        return actual;
    }

    public String helpGetNode(final String xmlFilePath, final String xpath, String namespaces ) throws IOException, XPathExpressionException, FunctionExecutionException {
        final String xmlContent = getContentOfTestFile(xmlFilePath);
        final Reader docReader = new StringReader(xmlContent);
        return XMLSystemFunctions.xpathValue(docReader,xpath, namespaces);
    }

    public void helpCheckElement(final Object jdomNode, final String name, final String prefix, final String namespaceUri,
                                 final String textContent) {
        assertTrue("Supplied JDOM node is not an Element", jdomNode instanceof Element); //$NON-NLS-1$
        final Element element = (Element)jdomNode;
        assertEquals(name, element.getName());
        assertEquals(prefix, element.getNamespacePrefix());
        assertEquals(namespaceUri, element.getNamespaceURI());

        final String actualTextContent = element.getText();
        if (textContent == null) {
            assertEquals(0, actualTextContent.length());
        } else {
            assertEquals(textContent, actualTextContent);
        }
    }

    public void helpCheckElement(final Object jdomNode, final String name, final String prefix, final String namespaceUri) {
        assertTrue("Supplied JDOM node is not an Element", jdomNode instanceof Element); //$NON-NLS-1$
        final Element element = (Element)jdomNode;
        assertEquals(name, element.getName());
        assertEquals(prefix, element.getNamespacePrefix());
        assertEquals(namespaceUri, element.getNamespaceURI());
    }

    public void helpCheckAttribute(final Object jdomNode, final String name,
                                 final String prefix, final String namespaceUri,
                                 final String value ) {
       assertTrue("Supplied JDOM node is not an Attribute",jdomNode instanceof Attribute); //$NON-NLS-1$
       final Attribute attribute = (Attribute)jdomNode;
       assertEquals(name,attribute.getName());
       assertEquals(prefix,attribute.getNamespacePrefix());
       assertEquals(namespaceUri,attribute.getNamespaceURI());
       
       final String actualTextContent = attribute.getValue();
       assertEquals(value,actualTextContent);
   }

    @Test public void testElement() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testAttribute() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b c=\"test\"></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/@c"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testText() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testNoMatch() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "x"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals(null, value);
    }

    @Test public void testNoXMLHeader() throws Exception {
        String doc = "<a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    // simulate what would happen if someone passed the output of an XML query to the xpathvalue function
    @Test public void testXMLInput() throws Exception {
        XMLType doc = new XMLType(new SQLXMLImpl("<foo/>"));//$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        String value = XMLSystemFunctions.xpathValue(doc, xpath);
        assertNull(value);
    }
    
    @Test(expected=XPathExpressionException.class) public void testBadXPath() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = ":BOGUS:"; //$NON-NLS-1$
        XMLSystemFunctions.xpathValue(doc, xpath);
    }
    
    @Test(expected=XPathExpressionException.class) public void testValidateXpath_Defect15088() throws Exception {
        // Mismatched tick and quote
        final String xpath = "//*[local-name()='bookName\"]"; //$NON-NLS-1$       
    	XMLSystemFunctions.validateXpath(xpath);
    }

    @Test public void testValidateXpath_null() throws Exception {
        XMLSystemFunctions.validateXpath(null);
    }

    @Test public void testValidateXpath_valid() throws Exception {
    	XMLSystemFunctions.validateXpath("//shipTo/@country"); //$NON-NLS-1$
    }

    @Test public void testGetSingleMatch_01_001() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//shipTo/@country"; //$NON-NLS-1$
        final String expectedValue = "US"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null, expectedValue);
    }

    @Test public void testGetSingleMatch_01_002() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//@partNum"; //$NON-NLS-1$
        final String expectedValue = "872-AA"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null, expectedValue);
    }

    @Test public void testGetSingleMatch_01_003() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//productName"; //$NON-NLS-1$
        final String expectedValue = "Lawnmower"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null, expectedValue);
    }

    @Test public void testGetSingleMatch_01_004() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "/SOAP-ENV:Envelope/billTo/zip"; //$NON-NLS-1$
        final String expectedValue = "95819"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,"xmlns:SOAP-ENV=\"http://schemas.xmlsoap.org/soap/envelope/\"", expectedValue);
    }

    @Test public void testGetSingleMatch_03() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*[local-name()=\"ReadOnly\"]"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null, "false"); //$NON-NLS-1$
    }
    
    /**
     * * is no longer valid to match the namespace
     */
    @Test(expected=XPathExpressionException.class) public void testGetSingleMatch_04() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*:ReadOnly"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null, "false"); //$NON-NLS-1$
    }
    
	@Test public void testInvokeXmlElement2() throws Exception {
		assertEquals("1969-12-31T18:00:00", XMLSystemFunctions.getStringValue(new Timestamp(0)));
    }
	
	@BeforeClass static public void setUpOnce() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT-6:00"));
	}
	
	@AfterClass static public void tearDownOnce() {
		TimeZone.setDefault(null);
	}

}
