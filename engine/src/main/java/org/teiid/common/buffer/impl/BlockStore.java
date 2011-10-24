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
import java.io.InputStream;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;

/**
 * Represents a FileStore that holds blocks of a fixed size.
 */
class BlockStore {
	final long blockSize;
	final ConcurrentBitSet blocksInUse;
	final FileStore[] stores;
	final ReentrantReadWriteLock[] locks;
	
	public BlockStore(StorageManager storageManager, int blockSize, int blockCountLog, int concurrencyLevel) {
		this.blockSize = blockSize;
		int blockCount = 1 << blockCountLog;
		this.blocksInUse = new ConcurrentBitSet(blockCount, concurrencyLevel);
		this.blocksInUse.setCompact(true);
		this.stores = new FileStore[concurrencyLevel];
		this.locks = new ReentrantReadWriteLock[concurrencyLevel];
		for (int i = 0; i < stores.length; i++) {
			this.stores[i] = storageManager.createFileStore(String.valueOf(blockSize) + '_' + i);
			this.locks[i] = new ReentrantReadWriteLock();
		}
		
	}
	
	int getAndSetNextClearBit(PhysicalInfo info) {
		int result = blocksInUse.getAndSetNextClearBit();
		if (result == -1) {
			throw new TeiidRuntimeException("Out of blocks of size " + blockSize); //$NON-NLS-1$
		}
		if (LogManager.isMessageToBeRecorded(LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
			LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "Allocating storage data block", result, "of size", blockSize, "to", info); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ 
		}
		return result;
	}
	
	int writeToStorageBlock(PhysicalInfo info,
			InputStream is) throws IOException {
		int block = getAndSetNextClearBit(info);
		int segment = block/blocksInUse.getBitsPerSegment();
		boolean success = false;
		this.locks[segment].writeLock().lock();
		try {
			FileStore fs = stores[segment];
			long blockOffset = (block%blocksInUse.getBitsPerSegment())*blockSize;
			//TODO: there is still an extra buffer being created here, we could FileChannels to do better
			byte[] b = new byte[BufferFrontedFileStoreCache.BLOCK_SIZE];
			int read = 0;
			while ((read = is.read(b, 0, b.length)) != -1) {
				fs.write(blockOffset, b, 0, read);
				blockOffset+=read;
			}
			success = true;
		} finally {
			locks[segment].writeLock().unlock();
			if (!success) {
				blocksInUse.clear(block);
				block = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
			}
		}
		return block;
	}

}