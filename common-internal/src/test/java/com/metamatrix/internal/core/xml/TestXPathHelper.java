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

package com.metamatrix.internal.core.xml;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import junit.framework.TestCase;

import net.sf.saxon.trans.XPathException;

import org.jdom.Attribute;
import org.jdom.Element;

import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.UnitTestUtil;


/** 
 * @since 4.2
 */
public class TestXPathHelper extends TestCase {

    /**
     * Constructor for TestXPathHelper.
     * @param name
     */
    public TestXPathHelper(String name) {
        super(name);
    }
    
    // =========================================================================
    //                      H E L P E R   M E T H O D S
    // =========================================================================

    public String getContentOfTestFile( final String testFilePath ) {
        final File file = UnitTestUtil.getTestDataFile(testFilePath);
        assertNotNull(file);
        assertEquals(true,file.exists());
        final FileUtil util = new FileUtil(file.getAbsolutePath());
        assertNotNull(util);
        return util.read();
    }
    
    public String helpTestXpathValue(final String xmlFilePath, final String xpath, 
                                     final String expected) throws XPathException, IOException {
        final String actual = helpGetNode(xmlFilePath,xpath);
        assertEquals(expected,actual);
        return actual;
    }

    public String helpGetNode(final String xmlFilePath, final String xpath ) throws XPathException, IOException {
        final String xmlContent = getContentOfTestFile(xmlFilePath);
        final Reader docReader = new StringReader(xmlContent);
        return XPathHelper.getSingleMatchAsString(docReader,xpath);
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

    // =========================================================================
    //                         T E S T     C A S E S
    // =========================================================================

    public void testValidateXpath_Defect15088() {
        // Mismatched tick and quote
        final String xpath = "//*[local-name()='bookName\"]"; //$NON-NLS-1$       
        try {
            XPathHelper.validateXpath(xpath);
            fail("Expected validation error but got none"); //$NON-NLS-1$
        } catch(XPathException e) {            
        }
    }

    public void testValidateXpath_null() throws Exception {
        XPathHelper.validateXpath(null);
    }

    public void testValidateXpath_valid() throws Exception {
        XPathHelper.validateXpath("//shipTo/@country"); //$NON-NLS-1$
    }

    public void testGetSingleMatch_01_001() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//shipTo/@country"; //$NON-NLS-1$
        final String expectedValue = "US"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,expectedValue);
    }

    public void testGetSingleMatch_01_002() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//@partNum"; //$NON-NLS-1$
        final String expectedValue = "872-AA"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,expectedValue);
    }

    public void testGetSingleMatch_01_003() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//productName"; //$NON-NLS-1$
        final String expectedValue = "Lawnmower"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,expectedValue);
    }

    public void testGetSingleMatch_01_004() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "/*:Envelope/billTo/zip"; //$NON-NLS-1$
        final String expectedValue = "95819"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,expectedValue);
    }

    public void testGetSingleMatch_02_001() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "/*:Envelope/*:HEADER/*:RequestID"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,null);
    }

    public void testGetSingleMatch_03() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*[local-name()=\"ReadOnly\"]"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,"false"); //$NON-NLS-1$
    }
    
    public void testGetSingleMatch_04() throws Exception {
        final String xmlFilePath = "testdoc.xml"; //$NON-NLS-1$
        final String xpath = "//*:ReadOnly"; //$NON-NLS-1$
        helpTestXpathValue(xmlFilePath,xpath,"false"); //$NON-NLS-1$
    }

}
