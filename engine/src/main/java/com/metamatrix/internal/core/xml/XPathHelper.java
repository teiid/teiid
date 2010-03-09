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

import java.io.IOException;
import java.io.Reader;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.om.Item;
import net.sf.saxon.sxpath.XPathEvaluator;
import net.sf.saxon.sxpath.XPathExpression;
import net.sf.saxon.trans.XPathException;

import com.metamatrix.core.util.ArgCheck;


/** 
 * A collection of methods to help with evaluating an XPath against a document.  These
 * utilities currently use Jaxen for XPath implementation.
 * @since 4.2
 */
public class XPathHelper {

    /**
     * Get the first XPath match when evaluating an XPath on a document.  If nothing is
     * matched, null is returned.
     * @param documentReader A reader for the XML document.  The reader will always be closed by this method.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     * @return String representation of the first matching item or null if none
     * @throws IllegalArgumentException If <code>documentReader</code> or <code>xpath</code> is null
     * @throws XPathException If an error occurs evaluating the XPath
     * @throws IOException If an error occurs closing the reader
     * @since 4.2
     */
    public static String getSingleMatchAsString(Reader documentReader, String xpath) throws XPathException, IOException {        
        Object o = getSingleMatch(documentReader, xpath);
        
        if(o == null) {
            return null;
        }
        
        // Return string value of node type
        if(o instanceof Item) {
            return ((Item)o).getStringValue();
        }  
        
        // Return string representation of non-node value
        return o.toString();
    }

    /**
     * Get the first XPath match when evaluating an XPath on a document.  If nothing is
     * matched, null is returned.
     * @param documentReader A reader for the XML document.  The reader will always be closed by this method.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     * @return String representation of the first matching item or null if none
     * @throws IllegalArgumentException If <code>documentReader</code> or <code>xpath</code> is null
     * @throws XPathException If an error occurs evaluating the XPath
     * @throws IOException If an error occurs closing the reader
     * @since 4.2
     */
    public static Object getSingleMatch(Reader documentReader, String xpath) throws XPathException, IOException {
        ArgCheck.isNotNull(documentReader);
        ArgCheck.isNotNull(xpath);

        try {
            Source s = new StreamSource(documentReader);
            XPathEvaluator eval = new XPathEvaluator();
            
            // Wrap the string() function to force a string return             
            XPathExpression expr = eval.createExpression(xpath);
            return expr.evaluateSingle(s);
            
        } finally {
            // Always close the reader
            documentReader.close();
        }
    }
    
    /**
     * Validate whether the XPath is a valid XPath.  If not valid, an XPathException will be thrown.
     * @param xpath An xpath expression, for example: a/b/c/getText()
     * @throws XPathException If an error occurs parsing the xpath
     * @since 4.2
     */
    public static void validateXpath(String xpath) throws XPathException {
        if(xpath == null) { 
            return;
        }
        
        XPathEvaluator eval = new XPathEvaluator();
        eval.createExpression(xpath);       // throws XPathException
    }

}
