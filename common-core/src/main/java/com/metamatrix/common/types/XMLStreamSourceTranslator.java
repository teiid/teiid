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

import java.io.CharArrayReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;


/** 
 * This class converts the Stream Source XML feed into other types of 
 * XML Sources. 
 */
public class XMLStreamSourceTranslator extends BaseXMLTranslator {
    
    char[] srcString;
    XMLReaderFactory readerFactory;
    
    public XMLStreamSourceTranslator(String xmlSource, Properties props) {
        super(props);
        this.srcString = xmlSource.toCharArray();
        this.readerFactory = new CharArrayXMLReader(this.srcString);
    }    
    
    public XMLStreamSourceTranslator(char[] xmlSource, Properties props) {
        super(props);
        this.srcString = xmlSource;
        this.readerFactory = new CharArrayXMLReader(this.srcString);
    }  
    
    public XMLStreamSourceTranslator(final StreamSource source, Properties props) {
        super(props);
        this.readerFactory = new StreamSourceXMLReader(source);
    }    
    
    public XMLStreamSourceTranslator(XMLReaderFactory factory, Properties props) {
        super(props);
        this.readerFactory = factory;
    }
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getSource()
     */
    public Source getSource() throws IOException {
        return new StreamSource(this.readerFactory.getReader());
    }
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getReader()
     */
    public Reader getReader() throws IOException {
        StreamSource src = (StreamSource)getSource();        
        if (src.getReader() != null) {
            return src.getReader();
        }     
        return super.getReader();        
    }  
    
    /** 
     * @see com.metamatrix.common.types.XMLTranslator#getInputStream()
     */
    public InputStream getInputStream() throws IOException {
        StreamSource src = (StreamSource)getSource();
        if (src.getInputStream() != null) {
            return src.getInputStream();
        }
        return super.getInputStream();
    }

    /**  
     * @see com.metamatrix.common.types.XMLTranslator#getString()
     */
    public String getString() throws IOException {
        if (this.srcString != null) {
            return new String(this.srcString);
        }
        return super.getString();
    }
    
    static class CharArrayXMLReader implements XMLReaderFactory{
        char[] contents;
        
        public CharArrayXMLReader(char[] content) {
            this.contents = content;
        }
        
        public Reader getReader() throws IOException {
            return new CharArrayReader(contents);
        }        
    }
    
    static class StreamSourceXMLReader implements XMLReaderFactory {
        StreamSource source;
        
        public StreamSourceXMLReader(StreamSource source) {
            this.source = source;
        }
        
        public Reader getReader() throws IOException {
            InputStream stream = source.getInputStream();
            if (stream != null) {
                return new InputStreamReader(stream);
            }
            return source.getReader();
        }         
    }
}
