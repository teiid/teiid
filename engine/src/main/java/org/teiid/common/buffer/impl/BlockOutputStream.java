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
import java.nio.ByteBuffer;

final class BlockOutputStream extends
		ExtensibleBufferedOutputStream {
	private final BlockManager blockManager;
	int blockNum = -1;
	private final int maxBlocks;
	private final boolean allocate;
	
	static final IOException exceededMax = new IOException();  

	/**
	 * @param blockManager
	 * @param maxBlocks a max of -1 indicates use existing blocks
	 */
	BlockOutputStream(BlockManager blockManager, int maxBlocks) {
		this.blockManager = blockManager;
		this.allocate = maxBlocks != -1;
		this.maxBlocks = maxBlocks - 2; //convert to an index
	}
	
	@Override
	protected ByteBuffer newBuffer() throws IOException {
		if (!allocate) {
			return blockManager.getBlock(++blockNum);
		}
		if (blockNum > maxBlocks) {
			throw exceededMax;
		}
		return blockManager.allocateBlock(++blockNum);
	}
	
	@Override
	protected int flushDirect(int i) throws IOException {
		return i;
	}
}