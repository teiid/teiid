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
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;

import org.teiid.core.types.DataTypeManager;

/**
 * Implements a buffered {@link InputStream} for a given {@link Reader} and {@link Charset}
 */
public class ReaderInputStream extends InputStream {
	
	//even though we're dealing with chars, we'll use the same default
	private static final int DEFAULT_BUFFER_SIZE = DataTypeManager.MAX_LOB_MEMORY_BYTES;
	
	private final Reader reader;
	private Writer writer;
	private char[] charBuffer;
	private AccessibleByteArrayOutputStream out = new AccessibleByteArrayOutputStream();
	private boolean hasMore = true;
	private int pos;
	
	public ReaderInputStream(Reader reader, Charset charset) {
		this(reader, charset, DEFAULT_BUFFER_SIZE);
	}

	public ReaderInputStream(Reader reader, Charset charset, int bufferSize) {
		this.reader = reader;
		this.writer = new OutputStreamWriter(out, charset);
		this.charBuffer = new char[bufferSize];
	}

	@Override
	public int read() throws IOException {
		while (pos >= out.getCount()) {
			if (!hasMore) {
				return -1;
			}
			out.reset();
			pos = 0;
			int charsRead = reader.read(charBuffer);
			if (charsRead == -1) {
				writer.close();
	            hasMore = false;
				continue;
			}
			writer.write(charBuffer, 0, charsRead);
			writer.flush();
		}
		return out.getBuffer()[pos++] & 0xff;
	}
	
	@Override
	public void close() throws IOException {
		this.reader.close();
	}
}