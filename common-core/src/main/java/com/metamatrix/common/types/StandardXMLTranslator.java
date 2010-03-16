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
import java.io.StringReader;
import java.io.Writer;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

public class StandardXMLTranslator extends XMLTranslator {
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

    static final String XMLPI = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"; //$NON-NLS-1$ 
    static final String newLine = "\n"; //$NON-NLS-1$
    
    private Source source;
    private Properties properties;
        
    public StandardXMLTranslator(Source source, Properties props) {
    	this.source = source;
        this.properties = props;
    }
    
    @Override
    public void translate(Writer writer) throws TransformerException, IOException {
        Transformer t = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader(idenityTransform)));
        writer.write(XMLPI);
        if (useIndentation()) {
            writer.write(newLine); 
        }
        t.transform(source, new StreamResult(writer));
    }
        
    private boolean useIndentation() {
        if (properties != null) {
            return "yes".equalsIgnoreCase(properties.getProperty(INDENT)); //$NON-NLS-1$            
        }
        return false;
    }
}
