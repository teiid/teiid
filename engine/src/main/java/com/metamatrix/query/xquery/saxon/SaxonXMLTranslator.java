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

package com.metamatrix.query.xquery.saxon;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.query.QueryResult;

import com.metamatrix.common.types.XMLTranslator;


/** 
 * This converts the SAXOn based XML tree into another popular XMl formats like String,
 * DOM, SAX etc.
 */
public class SaxonXMLTranslator implements XMLTranslator {
    NodeInfo source;
    Properties properties;
    
    public SaxonXMLTranslator(NodeInfo source, Properties props) {
        this.source = source;
        this.properties = props;
    }
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getString()
     */
    public String getString() throws IOException {
        StringWriter writer = new StringWriter();
        Result result = new StreamResult(writer);
        try {
            QueryResult.serialize(source, result, this.properties, new Configuration());
        } catch (TransformerException e) {
            throw new IOException(e.getMessage());
        }
        StringBuffer buffer = writer.getBuffer();
        return buffer.toString();
    }

    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getSource()
     */
    public Source getSource() throws IOException {
        return getStreamSource();
    }

    /**
     * Get a Stream source from the SAXON's tiny tree format.
     */
    private StreamSource getStreamSource() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Result result = new StreamResult(out);
        try {
            QueryResult.serialize(source, result, this.properties, new Configuration());
        } catch (TransformerException e) {
            throw new IOException(e.getMessage());
        }
        out.close();
        return new StreamSource(new ByteArrayInputStream(out.toByteArray()));
    }

    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getReader()
     */
    public Reader getReader() throws IOException {
        return new StringReader(getString());
    }    
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getInputStream()
     */
    public InputStream getInputStream() throws IOException {
        return getStreamSource().getInputStream();
    }

    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getProperties()
     */
    public Properties getProperties() {
        return this.properties;
    }    
}
