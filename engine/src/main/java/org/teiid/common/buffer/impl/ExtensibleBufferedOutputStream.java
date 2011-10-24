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

package org.teiid.common.buffer.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ExtensibleBufferedOutputStream extends OutputStream {
	
    protected ByteBuffer buf;
    protected int bytesWritten;
    private int startPosition;
    
    public ExtensibleBufferedOutputStream() {
	}
    
    public void write(int b) throws IOException {
    	ensureBuffer();
		buf.put((byte)b);
    }

	private void ensureBuffer() throws IOException {
		if (buf != null) {
			if (buf.remaining() != 0) {
				return;
			}
			flush();
		}
		buf = newBuffer();
		startPosition = buf.position();
	}

    public void write(byte b[], int off, int len) throws IOException {
    	while (true) {
        	ensureBuffer();
    		int toCopy = Math.min(buf.remaining(), len);
    		buf.put(b, off, toCopy);
			len -= toCopy;
			off += toCopy;
			if (buf.remaining() > 0) {
				break;
			}
    	}
    }

	public void flush() throws IOException {
		if (buf != null) {
			int bytes = buf.position() - startPosition;
			if (bytes > 0) {
				bytesWritten += flushDirect(bytes);
			}
		}
		buf = null;
	}

	protected abstract ByteBuffer newBuffer() throws IOException;
	
	/**
	 * Flush up to i bytes where i is the current position of the buffer
	 */
	protected abstract int flushDirect(int i) throws IOException;
    
    @Override
    public void close() throws IOException {
		flush();
    }
    
    public int getBytesWritten() {
		return bytesWritten;
	}
    
}
