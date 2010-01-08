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

package com.metamatrix.query.processor.xml;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.FileStore.FileStoreOutputStream;
import com.metamatrix.common.types.InputStreamFactory;
import com.metamatrix.common.types.SQLXMLImpl;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLTranslator;


/** 
 * Utility methods to be used with the XML and XQuery processing.
 */
public class XMLUtil {
	
    /**
     * This method saves the given XML object to the buffer manager's disk process
     * Documents less than the maxMemorySize will be held directly in memory
     */
    public static SQLXMLImpl saveToBufferManager(BufferManager bufferMgr, XMLTranslator translator, int maxMemorySize) 
        throws MetaMatrixComponentException {        
        boolean success = false;
        final FileStore lobBuffer = bufferMgr.createFileStore("xml"); //$NON-NLS-1$
        try{  
        	FileStoreOutputStream fsos = lobBuffer.createOutputStream(maxMemorySize);
            Writer writer = new OutputStreamWriter(fsos, Streamable.ENCODING);
            translator.translate(writer);
            writer.close();
            if (!fsos.bytesWritten()) {
            	return new SQLXMLImpl(fsos.toByteArray());
            }
            success = true;
            return createSQLXML(lobBuffer);
        } catch(IOException e) {
            throw new MetaMatrixComponentException(e);
        } finally {
        	if (!success && lobBuffer != null) {
        		lobBuffer.remove();
        	}
        }
    }

	public static SQLXMLImpl createSQLXML(final FileStore lobBuffer) {
		SQLXMLImpl sqlXML = new SQLXMLImpl(new InputStreamFactory(Streamable.ENCODING) {
			@Override
			public InputStream getInputStream() throws IOException {
				//TODO: adjust the buffer size, and/or develop a shared buffer strategy
				return new BufferedInputStream(lobBuffer.createInputStream());
			}
			
			@Override
			public void free() throws IOException {
				lobBuffer.remove();
			}
		});     
		lobBuffer.setCleanupReference(sqlXML);
		return sqlXML;
	}
    
}