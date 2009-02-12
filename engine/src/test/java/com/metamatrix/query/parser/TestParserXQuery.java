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

package com.metamatrix.query.parser;

import junit.framework.TestCase;

import com.metamatrix.query.sql.lang.XQuery;

public class TestParserXQuery extends TestCase {

    /** test a typical XQuery */
    public void testXQuery() {
        String xQueryString = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>\r\n"; //$NON-NLS-1$

        XQuery xQuery = new XQuery(xQueryString, null);

        TestParser.helpTest( 
                 xQueryString,  
                 xQueryString,  
                 xQuery);
    }

    /** test a typical XQuery with multiple docs*/
    public void testXQueryWithMultipleDocs() {
        String xQueryString = "<Items><x>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</x><y>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</y></Items>\r\n"; //$NON-NLS-1$

        XQuery xQuery = new XQuery(xQueryString, null);

        TestParser.helpTest( 
                 xQueryString,  
                 xQueryString,  
                 xQuery);
    }

    /** test an XQuery that doesn't begin with the open bracket < */
    public void testXQuery2() {

        String xQueryString = 
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n"; //$NON-NLS-1$
                        

        XQuery xQuery = new XQuery(xQueryString, null);

        TestParser.helpTest(
                xQueryString,  
                xQueryString,  
                xQuery);
    }

    /** test a typical XQuery with OPTION */
    public void testXQueryWithOptionUppercase() {
        String xQueryString = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>"; //$NON-NLS-1$

        String expectedString = xQueryString + " OPTION DEBUG"; //$NON-NLS-1$

        XQuery xQuery = new XQuery(xQueryString, null);

        TestParser.helpTest( 
                 expectedString,  
                 expectedString,  
                 xQuery);
    }

    /** test a typical XQuery with OPTION */
    public void testXQueryWithOptionLowercase() {
        String xQueryString = "<Items>\r\n" + //$NON-NLS-1$
                        "{\r\n" + //$NON-NLS-1$
                        "for $x in doc(\"xmltest.doc9893\")//ItemName\r\n" + //$NON-NLS-1$
                        "return  <Item>{$x/text()}</Item>\r\n" + //$NON-NLS-1$
                        "}\r\n" + //$NON-NLS-1$
                        "</Items>"; //$NON-NLS-1$

        String xQueryStringWithOption = xQueryString + " option debug"; //$NON-NLS-1$
        String expectedString = xQueryString + " OPTION DEBUG"; //$NON-NLS-1$

        XQuery xQuery = new XQuery(xQueryString, null);

        TestParser.helpTest( 
                 xQueryStringWithOption,  
                 expectedString,  
                 xQuery);
    }
}
