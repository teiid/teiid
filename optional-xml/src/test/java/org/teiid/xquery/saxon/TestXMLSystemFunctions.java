/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.xquery.saxon;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.TimeZone;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;
import javax.sql.rowset.serial.SerialException;
import javax.xml.stream.EventFilter;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLEventWriter;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.events.EndElement;
import javax.xml.stream.events.XMLEvent;

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
import org.teiid.query.function.source.XMLSystemFunctions;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.query.util.CommandContext;

import net.sf.saxon.trans.XPathException;

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
        return XMLFunctions.xpathValue(xmlContent,xpath);
    }

    @Test public void testElement() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testAttribute() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b c=\"test\"></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/@c"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testText() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    @Test public void testNoMatch() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "x"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertEquals(null, value);
    }

    @Test public void testNoXMLHeader() throws Exception {
        String doc = "<a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    // simulate what would happen if someone passed the output of an XML query to the xpathvalue function
    @Test public void testXMLInput() throws Exception {
        XMLType doc = new XMLType(new SQLXMLImpl("<foo/>"));//$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        String value = XMLFunctions.xpathValue(doc, xpath);
        assertNull(value);
    }

    @Test(expected=XPathException.class) public void testBadXPath() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = ":BOGUS:"; //$NON-NLS-1$
        XMLFunctions.xpathValue(doc, xpath);
    }

    @Test(expected=TeiidProcessingException.class) public void testValidateXpath_Defect15088() throws Exception {
        // Mismatched tick and quote
        final String xpath = "//*[local-name()='bookName\"]"; //$NON-NLS-1$
        XMLFunctions.validateXpath(xpath);
    }

    @Test public void testValidateXpath_null() throws Exception {
        XMLFunctions.validateXpath(null);
    }

    @Test public void testValidateXpath_valid() throws Exception {
        XMLFunctions.validateXpath("//shipTo/@country"); //$NON-NLS-1$
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
        assertEquals("1910-04-01T07:01:02.000055Z", XQueryEvaluator.convertToAtomicValue(TimestampUtil.createTimestamp(10, 3, 1, 1, 1, 2, 55001)).getStringValue());
    }

    @Test public void testAtomicValueTime() throws Exception {
        assertEquals("16:03:01Z", XQueryEvaluator.convertToAtomicValue(TimestampUtil.createTime(10, 3, 1)).getStringValue());
    }

    @Test public void testAtomicValueDate() throws Exception {
        assertEquals("1920-03-03Z", XQueryEvaluator.convertToAtomicValue(TimestampUtil.createDate(20, 2, 3)).getStringValue());
    }

    @Test public void testNameEscaping() throws Exception {
        assertEquals("_x003A_b", XMLFunctions.escapeName(":b", true));
    }

    @Test public void testNameEscaping1() throws Exception {
        assertEquals("a_x005F_x", XMLFunctions.escapeName("a_x", true));
        assertEquals("_", XMLFunctions.escapeName("_", true));
        assertEquals("_a", XMLFunctions.escapeName("_a", true));
    }

    @Test public void testNameEscaping2() throws Exception {
        assertEquals("_x000A_", XMLFunctions.escapeName(new String(new char[] {10}), true));
    }

    @Test public void testNameEscaping3() throws Exception {
        assertEquals("𐀀", XMLFunctions.escapeName("𐀀", true));
        assertEquals("_x0F1000_", XMLFunctions.escapeName(new String(Character.toChars(0xF1000)), true));
    }

    @Test public void testJsonToXml() throws Exception {
        String json = "[0,{\"1\":{\"2\":{\"3\":{\"4\":[5,{\"6\":7}]}}}}]";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Array xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Array xsi:type=\"decimal\">0</Array><Array><_x0031_><_x0032_><_x0033_><_x0034_ xsi:type=\"decimal\">5</_x0034_><_x0034_><_x0036_ xsi:type=\"decimal\">7</_x0036_></_x0034_></_x0033_></_x0032_></_x0031_></Array></Array>";
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
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><firstName>John</firstName><lastName>Smith</lastName><age xsi:type=\"decimal\">25</age><address><streetAddress>21 2nd Street</streetAddress><city>New York</city><state>NY</state><postalCode>10021</postalCode></address><phoneNumber><type>home</type><number>212 555-1234</number></phoneNumber><phoneNumber><type>fax</type><number>646 555-4567</number></phoneNumber></Person>";
        helpTestJson(json, "Person", expected);
    }

    @Test public void testJsonToXml2() throws Exception {
        String json = "{ \"firstName\": null }";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><firstName xsi:nil=\"true\"></firstName></Person>";
        helpTestJson(json, "Person", expected);
    }

    @Test public void testJsonToXml3() throws Exception {
        String json = "{ \"kids\":[{ \"firstName\" : \"George\" }, { \"firstName\" : \"Jerry\" }]}";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><kids><firstName>George</firstName></kids><kids><firstName>Jerry</firstName></kids></Person>";
        helpTestJson(json, "Person", expected);
    }

    @Test public void testJsonToXml4() throws Exception {
        String json = "{ \"kids\":[[{ \"firstName\" : \"George\" }, { \"firstName\" : \"Jerry\" }]]}";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><kids><kids><firstName>George</firstName></kids><kids><firstName>Jerry</firstName></kids></kids></Person>";
        helpTestJson(json, "Person", expected);
    }

    @Test public void testJsonToXml4a() throws Exception {
        String json = "{ \"kids\":[[{ \"firstName\" : \"George\" }], [{ \"firstName\" : \"Jerry\" }]]}";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><kids><kids><firstName>George</firstName></kids></kids><kids><kids><firstName>Jerry</firstName></kids></kids></Person>";
        helpTestJson(json, "Person", expected);
    }

    /**
     * This shows an ambiguity with the approach in that array/object children of an array cannot be distinguished
     * @throws Exception
     */
    @Test public void testJsonToXml5() throws Exception {
        String json = "[[],{\"x\": 1},[]]";
        String expected = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Person xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"><Person></Person><Person><x xsi:type=\"decimal\">1</x></Person><Person></Person></Person>";
        helpTestJson(json, "Person", expected);
    }

    @Test public void testRepairingNamespaces() throws Exception {
        XMLOutputFactory factory = XMLSystemFunctions.getOutputFactory(true);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        XMLEventWriter writer = factory.createXMLEventWriter(baos);
        XMLEventReader reader = XMLType.getXmlInputFactory().createXMLEventReader(new StringReader("<a xmlns:x=\"http://foo\"><b x:y=\"1\"/></a>"));
        reader.nextTag();
        reader = XMLType.getXmlInputFactory().createFilteredReader(reader, new EventFilter() {

            @Override
            public boolean accept(XMLEvent arg0) {
                if (arg0.isStartDocument() || arg0.isEndDocument()) {
                    return false;
                }
                if (arg0.isEndElement() && ((EndElement)arg0).getName().getLocalPart().equals("a")) {
                    return false;
                }
                return true;
            }
        });
        writer.add(reader);
        writer.close();
        assertEquals("<b xmlns=\"\" xmlns:x=\"http://foo\" x:y=\"1\"></b>", new String(baos.toByteArray(), "UTF-8"));
    }

    @BeforeClass static public void setUpOnce() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-6:00"));
    }

    @AfterClass static public void tearDownOnce() {
        TimeZone.setDefault(null);
    }

}
