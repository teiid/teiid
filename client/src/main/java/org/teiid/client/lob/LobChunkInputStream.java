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

package org.teiid.client.lob;

import java.io.IOException;
import java.io.InputStream;

import org.teiid.core.CorePlugin;



/** 
 * A InputStream wrapper class for a Lob Chunks. Given a stream of Lob Chunks
 * this class will convert those chunks into InputStream, which can be used to
 * stream the lob data. 
 */
public class LobChunkInputStream extends InputStream {
    private LobChunkProducer reader;

    private byte[] byteData = null;
    private int currentCounter = 0;
    private boolean lastChunk = false;
    private boolean closed = false;
    
    public LobChunkInputStream(LobChunkProducer reader) {
        this.reader = reader;
    }
    
    public int read() throws IOException {               
        if (this.closed) {
            throw new IllegalStateException(CorePlugin.Util.getString("stream_closed")); //$NON-NLS-1$
        }        
        while (this.byteData == null || this.byteData.length <= currentCounter) {
        	if (this.lastChunk) {
	            // we are done
	            return -1;
        	}
        	LobChunk value = this.reader.getNextChunk();
            this.lastChunk = value.isLast();
            this.byteData = value.getBytes();
            this.currentCounter = 0;
        }

        // so we have data
        return (byteData[currentCounter++] & 0xFF);
    }

    /**  
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        this.closed = true;
        this.reader.close();
    }

}
