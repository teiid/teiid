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

package org.teiid.common.buffer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public abstract class ExtensibleBufferedInputStream extends InputStream {
	ByteBuffer buf;

	@Override
	public int read() throws IOException {
		if (!ensureBytes()) {
			return -1;
		}
		return buf.get() & 0xff;
	}

	private boolean ensureBytes() throws IOException {
		if (buf == null || buf.remaining() == 0) {
			buf = nextBuffer();
			if (buf == null) {
				return false;
			}
		}
		return true;
	}

	protected abstract ByteBuffer nextBuffer() throws IOException;

	@Override
	public int read(byte[] b, int off, int len) throws IOException {
		if (!ensureBytes()) {
			return -1;
		}
		len = Math.min(len, buf.remaining());
		buf.get(b, off, len);
		return len;
	}
	
	@Override
	public void reset() throws IOException {
		if (buf != null) {
			buf.rewind();
		}
	}
	
}