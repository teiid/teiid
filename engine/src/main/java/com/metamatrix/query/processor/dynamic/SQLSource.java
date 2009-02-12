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

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.sax.SAXSource;

import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleSource;


/** 
 * This is merely a wrapper class which brings together the TupleInputSource and
 * TupleXMLReader classes together to build SAXSource object.
 */
public class SQLSource extends SAXSource {
    
    public static SAXSource createSource(String[] columns, Class[] types, TupleSource source) throws  MetaMatrixProcessingException, MetaMatrixComponentException {

        try {
            // get the sax parser and the its XML reader and replace with 
            // our own. and then supply the customized input source.            
            SAXParserFactory spf = SAXParserFactory.newInstance();
            spf.setNamespaceAware(true);            
            SAXParser sp = spf.newSAXParser();            
            XMLReader reader = sp.getXMLReader();
            
            return new SAXSource(new TupleXMLReader(reader), new TupleInputSource(columns, types, source));
            
        } catch (ParserConfigurationException e) {
            throw new MetaMatrixComponentException(e);
        } catch (SAXException e) {
            throw new MetaMatrixProcessingException(e);
        }        
    }
}
