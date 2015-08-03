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

/**
 * 
 */
package org.teiid.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import org.teiid.core.CorePlugin;

/**
 * Implements a buffered {@link InputStream} for a given {@link Reader} and {@link Charset}
 */
public class ReaderInputStream extends InputStream {
	
	//even though we're dealing with chars, we'll use the same default
	static final int DEFAULT_BUFFER_SIZE = 1<<13;
	
	private final Reader reader;
	private CharBuffer cb;
	private ByteBuffer bb;
	private boolean done;
	private boolean wasOverflow;
	private CharsetEncoder encoder;
	private byte[] singleByte = new byte[1];
	
	/**
	 * Creates a new inputstream that will replace any malformed/unmappable input
	 * @param reader
	 * @param charset
	 */
	public ReaderInputStream(Reader reader, Charset charset) {
		this(reader, charset.newEncoder()
				.onMalformedInput(CodingErrorAction.REPLACE)
				.onUnmappableCharacter(CodingErrorAction.REPLACE), 
				DEFAULT_BUFFER_SIZE);
	}
	
	public ReaderInputStream(Reader reader, CharsetEncoder encoder) {
		this(reader, encoder, DEFAULT_BUFFER_SIZE);
	}

	public ReaderInputStream(Reader reader, CharsetEncoder encoder, int bufferSize) {
		this.reader = reader;
		this.encoder = encoder;
		this.encoder.reset();
		this.cb = CharBuffer.allocate(bufferSize);
		this.bb = ByteBuffer.allocate(bufferSize);
		this.bb.limit(0);
	}
	
	@Override
	public int read(byte[] bbuf, int off, int len) throws IOException {
		if ((off < 0) || (off > bbuf.length) || (len < 0) ||
            ((off + len) > bbuf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
		while (!done && !bb.hasRemaining()) {
			int read = 0;
			int pos = cb.position();
			if (!wasOverflow) {
		    	while ((read = reader.read(cb)) == 0) {
		    		//blocking read
		    	}
				cb.flip();
			}
			bb.clear();
			CoderResult cr = encoder.encode(cb, bb, read == -1);
			checkResult(cr);
			if (read == -1 && !wasOverflow) {
	    		cr = encoder.flush(bb);
				checkResult(cr);
				if (!wasOverflow) {
		    		done = true;
				}
	    	}
			if (!wasOverflow) {
				if (read != 0 && cb.position() != read + pos) {
					cb.compact();
		    	} else {
		    		cb.clear();
		    	}
			}
			bb.flip();
		}
		len = Math.min(len, bb.remaining());
		if (len == 0 && done) {
			return -1;
		}
		bb.get(bbuf, off, len);
		return len;
	}

	private void checkResult(CoderResult cr) throws IOException {
		if (cr.isOverflow()) {
			wasOverflow = true;
			assert bb.position() > 0;
		} else if (!cr.isUnderflow()) {
			try {
				cr.throwException();
			} catch (CharacterCodingException e) {
				throw new IOException(CorePlugin.Util.gs(CorePlugin.Event.TEIID10083, encoder.charset().displayName()), e);
			}
		} else {
			wasOverflow = false;
		}
	}
	
	@Override
	public int read() throws IOException {
		int read = read(singleByte, 0, 1);
		if (read == 1) {
			return singleByte[0] & 0xff;
		}
		assert read != 0;
		return -1;
	}
	
	@Override
	public void close() throws IOException {
		this.reader.close();
	}
}