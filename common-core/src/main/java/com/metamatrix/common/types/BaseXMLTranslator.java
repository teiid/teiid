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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

/** 
 * Implements some common menthods that can be shared. 
 */
public abstract class BaseXMLTranslator implements XMLTranslator {
    static final String XMLPI = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"; //$NON-NLS-1$ 
    static final String newLine = "\n"; //$NON-NLS-1$
    
    Properties properties;
        
    protected BaseXMLTranslator(Properties props) {
        this.properties = props;
    }
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getString()
     */
    public String getString() throws IOException {
       try {
            Transformer t = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader(idenityTransform)));
            StringWriter sw = new StringWriter();
            sw.write(XMLPI);
            if (useIndentation()) {
                sw.write(newLine); 
            }
            t.transform(getSource(), new StreamResult(sw));
            return sw.toString(); 
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
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
        return new ByteArrayInputStream(getBytes());
    }
    
    /** 
     * Get the XML contents in byte array form.
     */
    public byte[] getBytes() throws IOException {
        try {
            Transformer t = TransformerFactory.newInstance().newTransformer(new StreamSource(new StringReader(idenityTransform)));
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            stream.write(XMLPI.getBytes());
            if (useIndentation()) {
                stream.write(newLine.getBytes()); 
            }            
            t.transform(getSource(), new StreamResult(stream));
            return stream.toByteArray(); 
        } catch (Exception e) {
            throw new IOException(e.getMessage());
        }
    }

    public Properties getProperties() {
        return this.properties;
    }
    
    private boolean useIndentation() {
        if (getProperties() != null) {
            return "yes".equalsIgnoreCase(getProperties().getProperty(INDENT)); //$NON-NLS-1$            
        }
        return false;
    }
}
