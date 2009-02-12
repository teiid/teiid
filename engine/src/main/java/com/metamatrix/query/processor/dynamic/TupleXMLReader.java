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

package com.metamatrix.query.processor.dynamic;

import java.io.IOException;

import org.xml.sax.ContentHandler;
import org.xml.sax.DTDHandler;
import org.xml.sax.EntityResolver;
import org.xml.sax.ErrorHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;


/** 
 * This is a replacement XMl Reader for the SAX Source; so that we can 
 * customize this to read a tuple soruce and generate the SAX Events.
 * The parse method is overloaded with our specific implementation.
 * 
 * Also, note that this is decorator for the actual XML Reader of the 
 * parser.
 */
public class TupleXMLReader implements XMLReader {

    XMLReader reader;
    public TupleXMLReader(XMLReader reader) {
        this.reader = reader;
    }

    /** 
     * @see org.xml.sax.XMLReader#getContentHandler()
     */
    public ContentHandler getContentHandler() {
        return this.reader.getContentHandler();
    }
    
    /** 
     * @see org.xml.sax.XMLReader#getDTDHandler()
     */
    public DTDHandler getDTDHandler() {
        return this.reader.getDTDHandler();
    }
    /** 
     * @see org.xml.sax.XMLReader#getEntityResolver()
     */
    public EntityResolver getEntityResolver() {
        return this.reader.getEntityResolver();
    }
    /** 
     * @see org.xml.sax.XMLReader#getErrorHandler()
     */
    public ErrorHandler getErrorHandler() {
        return this.reader.getErrorHandler();
    }
    /** 
     * @see org.xml.sax.XMLReader#getFeature(java.lang.String)
     */
    public boolean getFeature(String name) 
        throws SAXNotRecognizedException, SAXNotSupportedException {
        return this.reader.getFeature(name);
    }
    /** 
     * @see org.xml.sax.XMLReader#getProperty(java.lang.String)
     */
    public Object getProperty(String name) 
        throws SAXNotRecognizedException, SAXNotSupportedException {
        return this.reader.getProperty(name);
    }
        
    /** 
     * @see org.xml.sax.XMLReader#parse(org.xml.sax.InputSource)
     */
    public void parse(InputSource input) 
        throws IOException, SAXException {
        if (input instanceof TupleInputSource) {
            TupleInputSource in = (TupleInputSource)input;
            in.parse(getContentHandler());
        }
        else {
            this.reader.parse(input);
        }
    }
    /** 
     * @see org.xml.sax.XMLReader#parse(java.lang.String)
     */
    public void parse(String systemId) 
        throws IOException, SAXException {
        this.reader.parse(systemId);
    }
    /** 
     * @see org.xml.sax.XMLReader#setContentHandler(org.xml.sax.ContentHandler)
     */
    public void setContentHandler(ContentHandler handler) {
        this.reader.setContentHandler(handler);
    }
    /** 
     * @see org.xml.sax.XMLReader#setDTDHandler(org.xml.sax.DTDHandler)
     */
    public void setDTDHandler(DTDHandler handler) {
        this.reader.setDTDHandler(handler);
    }
    /** 
     * @see org.xml.sax.XMLReader#setEntityResolver(org.xml.sax.EntityResolver)
     */
    public void setEntityResolver(EntityResolver resolver) {
        this.reader.setEntityResolver(resolver);
    }
    /** 
     * @see org.xml.sax.XMLReader#setErrorHandler(org.xml.sax.ErrorHandler)
     */
    public void setErrorHandler(ErrorHandler handler) {
        this.reader.setErrorHandler(handler);
    }
    /** 
     * @see org.xml.sax.XMLReader#setFeature(java.lang.String, boolean)
     */
    public void setFeature(String name, boolean value) 
        throws SAXNotRecognizedException, SAXNotSupportedException {
        this.reader.setFeature(name, value);
    }
    /** 
     * @see org.xml.sax.XMLReader#setProperty(java.lang.String, java.lang.Object)
     */
    public void setProperty(String name, Object value) 
        throws SAXNotRecognizedException, SAXNotSupportedException {
        this.reader.setProperty(name, value);
    }
}
