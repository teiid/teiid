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

package org.teiid.dqp.internal.process;

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.lob.LobChunkProducer;
import org.teiid.core.CorePlugin;


/**
 * A wrapper class, given a InputStream object can convert a underlying 
 * stream into sequence of ByteLobChunk objects of given chunk size. 
 */
public class ByteLobChunkStream implements LobChunkProducer {
    private PushbackInputStream stream;
    private int chunkSize;
    private boolean closed;
    
    public ByteLobChunkStream(InputStream stream, int chunkSize) {
        this.stream = new PushbackInputStream(stream);
        this.chunkSize = chunkSize;
    }
    
    public LobChunk getNextChunk() throws IOException{

        if (this.closed) {
            throw new IllegalStateException(CorePlugin.Util.getString("stream_closed")); //$NON-NLS-1$
        }
                
        // read contents from the stream
        byte[] cbuf = new byte[this.chunkSize];
        int read = this.stream.read(cbuf);
        if (read == -1) {
            return new LobChunk(new byte[0], true);
        }
        boolean isLast = false;
        if (read != this.chunkSize) {
            byte[] buf = new byte[read];
            System.arraycopy(cbuf, 0, buf, 0, read);
            cbuf = buf;
        }
        int next = this.stream.read();
        if (next == -1) {
        	isLast = true;
        } else {
        	this.stream.unread(next);
        }
        return new LobChunk(cbuf, isLast);
    }

    public void close() throws IOException {
        this.closed = true;
        this.stream.close();
    }        
}