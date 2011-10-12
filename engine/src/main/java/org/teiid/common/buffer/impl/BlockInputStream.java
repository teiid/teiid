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

import java.io.InputStream;
import java.nio.ByteBuffer;

final class BlockInputStream extends InputStream {
	private final BlockManager manager;
	private final int maxBlock;
	int blockIndex;
	ByteBuffer buf;
	boolean free;
	boolean done;

	BlockInputStream(BlockManager manager, int blockCount, boolean free) {
		this.manager = manager;
		this.free = free;
		this.maxBlock = blockCount;
	}

	@Override
	public int read() {
		ensureBytes();
		if (done) {
			return -1;
		}
		return buf.get() & 0xff;
	}

	private void ensureBytes() {
		if (buf == null || buf.remaining() == 0) {
			if (maxBlock == blockIndex) {
				done = true;
				if (blockIndex > 1 && free) {
					manager.freeBlock(blockIndex - 1, false);
				}
				return;
			}
			buf = manager.getBlock(blockIndex++);
			if (blockIndex > 2 && free) {
				manager.freeBlock(blockIndex - 2, false);
			}
		}
	}

	@Override
	public int read(byte[] b, int off, int len) {
		ensureBytes();
		if (done) {
			return -1;
		}
		len = Math.min(len, buf.remaining());
		buf.get(b, off, len);
		return len;
	}
	
	public int free(boolean steal) {
		return manager.free(steal);
	}
}