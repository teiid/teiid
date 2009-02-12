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

import java.io.StringReader;
import java.util.List;
import java.util.Properties;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.types.InvalidReferenceException;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.query.processor.xml.XMLUtil;


/** 
 * This class will be a reader class for reading data from the XML Source realated
 * connectors. 
 */
public class XMLSource {
    private static final String NO_RESULTS_DOCUMENT = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><results/>"; //$NON-NLS-1$
    
    public static Source createSource(String[] columns, Class[] types, TupleSource source, BufferManager bufferMgr) 
        throws  MetaMatrixProcessingException {

        try {
            // we only want to return the very first document from the result set
            // as XML we expect in doc function to have single XML document
            List tuple = source.nextTuple();
            if (tuple != null) {                        
                Object value = tuple.get(0);
                if (value != null) {
                    // below we will catch any invalid LOB refereces and return them
                    // as processing excceptions.
                    if (value instanceof XMLType) {
                        XMLType xml = (XMLType)value;
                        try {
                            return xml.getSource(null);
                        } catch (InvalidReferenceException e) {
                            xml = XMLUtil.getFromBufferManager(bufferMgr, new TupleSourceID(xml.getPersistenceStreamId()), new Properties());
                            return xml.getSource(null);
                        } 
                    }
                    return new StreamSource(new StringReader((String)value));
                }
            }
        } catch (Exception e) { 
            throw new MetaMatrixProcessingException(e);
        }
        return new StreamSource(new StringReader(NO_RESULTS_DOCUMENT));
    }    
}
