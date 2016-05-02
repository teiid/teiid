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

package org.teiid.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.teiid.core.types.DataTypeManager;

/**
 * Replacement for the standard {@link java.io.InputStreamReader}, 
 * which suffers from a <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4744247">bug</a> in sun.nio.cs.StreamDecoder
 */
public class InputStreamReader extends Reader {

	private CharsetDecoder cd;
	private ReadableByteChannel rbc;
	private ByteBuffer bb;
	private CharBuffer cb;
	private boolean done;
	
	public InputStreamReader(InputStream in, CharsetDecoder cd) {
		this(in, cd, DataTypeManager.MAX_LOB_MEMORY_BYTES);
	}
	
	public InputStreamReader(InputStream in, CharsetDecoder cd, int bufferSize) {
		this.cd = cd;
		this.rbc = Channels.newChannel(in);
		this.bb = ByteBuffer.allocate(bufferSize);
		this.cb = CharBuffer.allocate((int)(bufferSize * (double)cd.maxCharsPerByte()));
		this.cb.limit(0);
	}
	
	@Override
	public void close() throws IOException {
		rbc.close();
		cd.reset();
	}

	@Override
	public int read(char[] cbuf, int off, int len) throws IOException {
		if ((off < 0) || (off > cbuf.length) || (len < 0) ||
            ((off + len) > cbuf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
		while (!done && !cb.hasRemaining()) {
			int read = 0;
			int pos = bb.position();
	    	while ((read = rbc.read(bb)) == 0) {
	    		//blocking read
	    	}
	    	bb.flip();
	    	cb.clear();
			CoderResult cr = cd.decode(bb, cb, read == -1);
			if (!cr.isUnderflow()) {
			    cr.throwException();
			}
	    	if (read == -1) {
	    		cr = cd.flush(cb);
	    		if (!cr.isUnderflow()) {
	    			cr.throwException();
	    		}
	    		done = true;
	    	}
	    	if (bb.position() != read + pos) {
	    		bb.compact();
	    	} else {
	    		bb.clear();
	    	}
    		cb.flip();
		}
		len = Math.min(len, cb.remaining());
		if (len == 0 && done) {
			return -1;
		}
		cb.get(cbuf, off, len);
		return len;
	}

}