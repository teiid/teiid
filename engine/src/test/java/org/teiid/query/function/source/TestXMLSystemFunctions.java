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
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;

import net.sf.saxon.trans.XPathException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.XMLType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestXMLSystemFunctions {
	
    public String getContentOfTestFile( final String testFilePath ) throws IOException {
        final File file = UnitTestUtil.getTestDataFile(testFilePath);
        return ObjectConverterUtil.convertFileToString(file);
    }
    
    public String helpTestXpathValue(final String xmlFilePath, final String xpath, final String expected) throws XPathException, TeiidProcessingException, IOException {
        final String actual = helpGetNode(xmlFilePath,xpath);
        assertEquals(expected,actual);
        return actual;
    }

    public String helpGetNode(final String xmlFilePath, final String xpath ) throws XPathException, TeiidProcessingException, IOException {
        final String xmlContent = getContentOfTestFile(xmlFilePath);
        return XMLSystemFunctions.xpathValue(xmlContent,xpath);
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
    
    @Test(expected=XPathException.class) public void testBadXPath() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = ":BOGUS:"; //$NON-NLS-1$
        XMLSystemFunctions.xpathValue(doc, xpath);
    }
    
    @Test(expected=XPathException.class) public void testValidateXpath_Defect15088() throws Exception {
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
        helpTestXpathValue(xmlFilePath,xpath, expectedValue);
    }

    @Test public void testGetSingleMatch_01_002() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//@partNum"; //$NON-NLS-1$
        final String expectedValue = "872-AA"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath, expectedValue);
    }

    @Test public void testGetSingleMatch_01_003() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//productName"; //$NON-NLS-1$
        final String expectedValue = "Lawnmower"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath, expectedValue);
    }

    @Test public void testGetSingleMatch_03() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*[local-name()=\"ReadOnly\"]"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath, "false"); //$NON-NLS-1$
    }
    
    /**
     * * is no longer valid to match the namespace
     */
    @Test public void testGetSingleMatch_04() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*:ReadOnly"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath, "false"); //$NON-NLS-1$
    }
    
	@Test public void testAtomicValueTimestamp() throws Exception {
		assertEquals("1910-04-01T07:01:02.000055Z", XMLSystemFunctions.convertToAtomicValue(TimestampUtil.createTimestamp(10, 3, 1, 1, 1, 2, 55001)).getStringValue());
    }
	
	@Test public void testAtomicValueTime() throws Exception {
		assertEquals("16:03:01Z", XMLSystemFunctions.convertToAtomicValue(TimestampUtil.createTime(10, 3, 1)).getStringValue());
    }

	@Test public void testAtomicValueDate() throws Exception {
		assertEquals("1920-03-03Z", XMLSystemFunctions.convertToAtomicValue(TimestampUtil.createDate(20, 2, 3)).getStringValue());
    }
	
	@Test public void testNameEscaping() throws Exception {
		assertEquals("_u003A_b", XMLSystemFunctions.escapeName(":b", true));
    }
	
	@Test public void testNameEscaping1() throws Exception {
		assertEquals("a_u005F_x", XMLSystemFunctions.escapeName("a_x", true));
    }
	
	@Test public void testNameEscaping2() throws Exception {
		assertEquals("_u000A_", XMLSystemFunctions.escapeName(new String(new char[] {10}), true));
    }
	
	@Test public void testJsonToXml() throws Exception {
		String json = "[0,{\"1\":{\"2\":{\"3\":{\"4\":[5,{\"6\":7}]}}}}]";
		String expected = "<?xml version=\"1.0\" ?><Array><Array>0</Array><Array><_u0031_><_u0032_><_u0033_><_u0034_><_u0034_>5</_u0034_><_u0034_><_u0036_>7</_u0036_></_u0034_></_u0034_></_u0033_></_u0032_></_u0031_></Array></Array>";
		helpTestJson(json, "Array", expected);
	}

	private void helpTestJson(String json, String rootName, String expected)
			throws SQLException, TeiidComponentException,
			TeiidProcessingException, SerialException, IOException {
		CommandContext cc = new CommandContext();
		cc.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
		SQLXML xml = XMLSystemFunctions.jsonToXml(cc, rootName, new SerialClob(json.toCharArray()));
		assertEquals(expected, xml.getString());
		xml = XMLSystemFunctions.jsonToXml(cc, rootName, new SerialBlob(json.getBytes(Charset.forName("UTF-8"))));
		assertEquals(expected, xml.getString());
		xml = XMLSystemFunctions.jsonToXml(cc, rootName, new SerialBlob(json.getBytes(Charset.forName("UTF-32BE"))));
		assertEquals(expected, xml.getString());
	}
	
	@Test public void testJsonToXml1() throws Exception {
		String json = "{ \"firstName\": \"John\", \"lastName\": \"Smith\", \"age\": 25, \"address\": { \"streetAddress\": \"21 2nd Street\", \"city\": \"New York\", \"state\": \"NY\", "+
		         "\"postalCode\": \"10021\" }, \"phoneNumber\": [ { \"type\": \"home\", \"number\": \"212 555-1234\" }, { \"type\": \"fax\", \"number\": \"646 555-4567\" } ] }"; 
		String expected = "<?xml version=\"1.0\" ?><Person><firstName>John</firstName><lastName>Smith</lastName><age>25</age><address><streetAddress>21 2nd Street</streetAddress><city>New York</city><state>NY</state><postalCode>10021</postalCode></address><phoneNumber><phoneNumber><type>home</type><number>212 555-1234</number></phoneNumber><number><type>fax</type><number>646 555-4567</number></number></phoneNumber></Person>";
		helpTestJson(json, "Person", expected);
	}
	
	@Test public void testJsonToXml2() throws Exception {
		String json = "{ \"firstName\": null }"; 
		String expected = "<?xml version=\"1.0\" ?><Person><firstName xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:nil=\"true\"></firstName></Person>";
		helpTestJson(json, "Person", expected);
	}
	
	@BeforeClass static public void setUpOnce() {
		TimeZone.setDefault(TimeZone.getTimeZone("GMT-6:00"));
	}
	
	@AfterClass static public void tearDownOnce() {
		TimeZone.setDefault(null);
	}

}
