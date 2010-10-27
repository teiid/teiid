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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;

import javax.xml.transform.TransformerException;

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLTranslator;

/** 
 * Utility methods to be used with the XML and XQuery processing.
 */
public class XMLUtil {
	
	public static final class FileStoreInputStreamFactory extends InputStreamFactory {
		private final FileStore lobBuffer;
		private final FileStoreOutputStream fsos;
		private String encoding;

		public FileStoreInputStreamFactory(FileStore lobBuffer, String encoding) {
			this.encoding = encoding;
			this.lobBuffer = lobBuffer;
			fsos = lobBuffer.createOutputStream(DataTypeManager.MAX_LOB_MEMORY_BYTES);
			this.lobBuffer.setCleanupReference(this);
		}

		@Override
		public InputStream getInputStream() throws IOException {
			if (!fsos.bytesWritten()) {
				return new ByteArrayInputStream(fsos.getBuffer(), 0, fsos.getCount());
			}
			//TODO: adjust the buffer size, and/or develop a shared buffer strategy
			return new BufferedInputStream(lobBuffer.createInputStream(0));
		}

		@Override
		public long getLength() {
			return lobBuffer.getLength();
		}

		public Writer getWriter() {
			return new OutputStreamWriter(fsos, Charset.forName(encoding));
		}
		
		public FileStoreOutputStream getOuputStream() {
			return fsos;
		}

		@Override
		public void free() throws IOException {
			lobBuffer.remove();
		}
	}

    /**
     * This method saves the given XML object to the buffer manager's disk process
     * Documents less than the maxMemorySize will be held directly in memory
     */
    public static SQLXMLImpl saveToBufferManager(BufferManager bufferMgr, XMLTranslator translator) 
        throws TeiidComponentException, TeiidProcessingException {        
        boolean success = false;
        final FileStore lobBuffer = bufferMgr.createFileStore("xml"); //$NON-NLS-1$
        FileStoreInputStreamFactory fsisf = new FileStoreInputStreamFactory(lobBuffer, Streamable.ENCODING);
        try{  
        	Writer writer = fsisf.getWriter();
            translator.translate(writer);
            writer.close();
            success = true;
            return new SQLXMLImpl(fsisf);
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

}