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

import java.util.List;

import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.common.xml.XmlUtil;


/** 
 * A extend class which defines the "InputSource" interface on a
 * MetaMatrix's TupleSource. This will be used as in SaxSource's 
 * input source object to represent the tuple as sax stream.
 */
public class TupleInputSource extends InputSource {
    static final String nsURI = ""; //$NON-NLS-1$
    static final AttributesImpl atts = new AttributesImpl();
    private static final String ROW = "row"; //$NON-NLS-1$
    private static final String RESULTS = "results"; //$NON-NLS-1$
    private static final String NULL = "null";//$NON-NLS-1$
    
    String[] columns;
    TupleSource source;
    Class[] types;
        
    public TupleInputSource(String[] columns, Class[] types, TupleSource source) {
        this.columns = columns;
        this.source = source;
        this.types = types;
    }
    
    /**
     * This will be called by the extended XMLReader to parse the
     * Tuple source. 
     * @param saxHandler
     */
    public void parse(ContentHandler saxHandler) throws SAXException{
        saxHandler.startDocument();        
        saxHandler.startElement(nsURI, RESULTS, RESULTS, atts);
        
        try {
            List tuple = this.source.nextTuple(); 
            while (tuple != null) {                                      
                saxHandler.startElement(nsURI, ROW, ROW, atts);
                
                // write each column of the row
                for(int i = 0; i < this.columns.length; i++) {
                    String element = this.columns[i];                            
                    
                    saxHandler.startElement(nsURI, element, element, atts); 
                    Object obj = tuple.get(i);
                    String value = NULL;
                    if (obj != null 
                        && types[i] != DataTypeManager.getDataTypeClass(DataTypeManager.DefaultDataTypes.CLOB) 
                        && types[i] != DataTypeManager.getDataTypeClass(DataTypeManager.DefaultDataTypes.BLOB)) {
                        value = XmlUtil.escapeCharacterData(obj.toString());                    
                    }
                    saxHandler.characters (value.toCharArray(), 0, value.length());
                    saxHandler.endElement(nsURI, element, element);                           
                }
                saxHandler.endElement(nsURI, ROW, ROW);

                // get the next tuple
                tuple = this.source.nextTuple();
            }        
            saxHandler.endElement(nsURI, RESULTS, RESULTS);                    
            saxHandler.endDocument();
            this.source.closeSource();
        } catch (MetaMatrixComponentException e) {
            throw new SAXException(e);
        } catch (MetaMatrixProcessingException e) {
        	throw new SAXException(e);
        }
    }       
}


