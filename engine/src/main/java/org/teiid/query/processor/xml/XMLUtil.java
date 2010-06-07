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

package org.teiid.query.processor.xml;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import javax.xml.transform.TransformerException;

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLTranslator;



/** 
 * Utility methods to be used with the XML and XQuery processing.
 */
public class XMLUtil {
	
	//horrible hack
	private static BufferManager bufferManager;
	
	public static void setBufferManager(BufferManager bufferManager) {
		XMLUtil.bufferManager = bufferManager;
	}
	
	public static SQLXMLImpl saveToBufferManager(XMLTranslator translator) throws TeiidComponentException, TeiidProcessingException {
		return saveToBufferManager(bufferManager, translator, Streamable.STREAMING_BATCH_SIZE_IN_BYTES);
	}
	
    /**
     * This method saves the given XML object to the buffer manager's disk process
     * Documents less than the maxMemorySize will be held directly in memory
     */
    public static SQLXMLImpl saveToBufferManager(BufferManager bufferMgr, XMLTranslator translator, int maxMemorySize) 
        throws TeiidComponentException, TeiidProcessingException {        
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
            throw new TeiidComponentException(e);
        } catch(TransformerException e) {
            throw new TeiidProcessingException(e);
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
				return new BufferedInputStream(lobBuffer.createInputStream(0));
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