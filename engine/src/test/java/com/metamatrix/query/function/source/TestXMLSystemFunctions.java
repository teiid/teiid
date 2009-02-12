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

package com.metamatrix.query.function.source;

import junit.framework.TestCase;

import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.XMLType;


/** 
 * @since 4.2
 */
public class TestXMLSystemFunctions extends TestCase {

    public void testElement() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        String value = (String) XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    public void testAttribute() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b c=\"test\"></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/@c"; //$NON-NLS-1$
        String value = (String) XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    public void testText() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = (String) XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    public void testNoMatch() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "x"; //$NON-NLS-1$
        String value = (String) XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals(null, value);
    }

    public void testNoXMLHeader() throws Exception {
        String doc = "<a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = "a/b/c/text()"; //$NON-NLS-1$
        String value = (String) XMLSystemFunctions.xpathValue(doc, xpath);
        assertEquals("test", value); //$NON-NLS-1$
    }

    // simulate what would happen if someone passed the output of an XML query to the xpathvalue function
    public void testXMLInput() throws Exception {
        XMLType doc = new XMLType(new SQLXMLImpl("<foo/>"));//$NON-NLS-1$
        String xpath = "a/b/c"; //$NON-NLS-1$
        try {
            XMLSystemFunctions.xpathValue(doc, xpath);
        } catch(FunctionExecutionException e) {
            // expected
            //System.out.println(e.getMessage());
        }            
    }
    
    public void testBadTypeInput() throws Exception {
        Integer doc = new Integer(1);
        String xpath = "a/b/c"; //$NON-NLS-1$
        try {
            XMLSystemFunctions.xpathValue(doc, xpath);
            fail("Expected exception passing xml stream to xpathvalue()"); //$NON-NLS-1$
        } catch(FunctionExecutionException e) {
            // expected
            //System.out.println(e.getMessage());
        }            
    }    
    
    public void testBadXPath() throws Exception {
        String doc = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><a><b><c>test</c></b></a>"; //$NON-NLS-1$
        String xpath = ":BOGUS:"; //$NON-NLS-1$
        try {
            XMLSystemFunctions.xpathValue(doc, xpath);
            fail("Expected exception passing bad xpath"); //$NON-NLS-1$
        } catch(FunctionExecutionException e) {
            // expected
        }                    
    }

}
