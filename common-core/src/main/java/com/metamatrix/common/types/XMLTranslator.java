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

package com.metamatrix.common.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import javax.xml.transform.Source;


/** 
 * This an interface defined to convert the various kinds of the XML sources
 * defined into another source kinds. For example a DOMSource.class source can be
 * converted to String or SAXSource, or StreamSource is converted to DOMSource etc.
 */
public interface XMLTranslator {
    static String lineSep = System.getProperty("line.separator"); //$NON-NLS-1$    
    public static final String INDENT = "indent"; //$NON-NLS-1$
    
    public static final String idenityTransform = 
            "<xsl:stylesheet version=\"2.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">" + lineSep + //$NON-NLS-1$
            "<xsl:output method = \"xml\" omit-xml-declaration=\"yes\"/>" + lineSep + //$NON-NLS-1$
            "<xsl:template match=\"@*|node()\">" + lineSep +//$NON-NLS-1$ 
            "    <xsl:copy>\r\n" + lineSep+ //$NON-NLS-1$
            "        <xsl:apply-templates select=\"@*|node()\"/>" +lineSep+//$NON-NLS-1$ 
            "    </xsl:copy>" + lineSep + //$NON-NLS-1$
            "</xsl:template>" + lineSep +//$NON-NLS-1$
            "</xsl:stylesheet>" + lineSep + //$NON-NLS-1$
            ""; //$NON-NLS-1$
    
    /**
     * Get String form of the XML  
     * @return string representing the XML source
     * @throws IOException
     */
    String getString() throws IOException;
    
    /**
     * Get the XML in the original source form; with however it got created. 
     * @return
     * @throws IOException
     */
    public Source getSource() throws IOException;
    
    
    /**
     * Get a Reader for the XML contents; 
     * @return a Reader object for streaming the XML contents.
     * @throws IOException
     */
    public Reader getReader() throws IOException;
    
    /**
     * Get a InputStream  for the XMl contents; 
     * @return a InputStream object for streaming the XML contents.
     * @throws IOException
     */
    public InputStream getInputStream() throws IOException;
    
    /**
     * Any specific Properties needed by the translator process, such
     * as indenting etc. 
     * @return properties
     */
    public Properties getProperties();
    
}
