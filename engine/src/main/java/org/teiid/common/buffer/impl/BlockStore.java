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

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;

class BlockStore {
	final long blockSize;
	final ConcurrentBitSet blocksInUse;
	final FileStore[] stores;
	
	public BlockStore(StorageManager storageManager, int blockSize, int blockCountLog) {
		this.blockSize = blockSize;
		int blockCount = 1 << blockCountLog;
		this.blocksInUse = new ConcurrentBitSet(blockCount, BufferManagerImpl.CONCURRENCY_LEVEL/2);
		this.stores = new FileStore[BufferManagerImpl.CONCURRENCY_LEVEL/2];
		for (int i = 0; i < stores.length; i++) {
			this.stores[i] = storageManager.createFileStore(String.valueOf(blockSize) + '_' + i); 
		}
	}

}