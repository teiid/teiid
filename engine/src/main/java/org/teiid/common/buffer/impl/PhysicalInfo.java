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

import org.teiid.common.buffer.BaseCacheEntry;
import org.teiid.common.buffer.CacheKey;
import org.teiid.core.TeiidRuntimeException;

/**
 * Represents the memory buffer and storage state of an object.
 * It is important to minimize the amount of data held here.
 * Currently should be 56 bytes.
 */
final class PhysicalInfo extends BaseCacheEntry {
	
	static final Exception sizeChanged = new Exception();  
	
	final Long gid;
	//the memory inode and block count
	int inode = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
	int memoryBlockCount;
	//the storage block and BlockStore index
	int block = BufferFrontedFileStoreCache.EMPTY_ADDRESS;
	byte sizeIndex = 0;
	//state flags
	boolean pinned; //indicates that the entry is being read
	boolean evicting; //indicates that the entry will be moved out of the memory buffer
	boolean loading; //used by tier 1 cache to prevent double loads
	boolean adding; //used to prevent double adds
	
	PhysicalInfo(Long gid, Long id, int inode, int lastAccess) {
		super(new CacheKey(id, lastAccess, 0));
		this.inode = inode;
		this.gid = gid;
	}
	
	void setSize(int size) throws Exception {
		int newMemoryBlockCount = (size>>BufferFrontedFileStoreCache.LOG_BLOCK_SIZE) + ((size&BufferFrontedFileStoreCache.BLOCK_MASK)>0?1:0);
		if (this.memoryBlockCount != 0) {
			if (newMemoryBlockCount != memoryBlockCount) {
				throw sizeChanged; 
			}
			return; //no changes
		}
		this.memoryBlockCount = newMemoryBlockCount;
		while (newMemoryBlockCount > 1) {
			this.sizeIndex++;
			newMemoryBlockCount = (newMemoryBlockCount>>1) + ((newMemoryBlockCount&0x01)==0?0:1);
		}
	}
	
	void await(boolean donePinning, boolean doneEvicting) {
		while ((donePinning && pinned) || (doneEvicting && evicting)) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new TeiidRuntimeException(e);
			}
		}
	}
	
	synchronized void lockForLoad() {
		while (loading) {
			try {
				wait();
			} catch (InterruptedException e) {
				throw new TeiidRuntimeException(e);
			}
		}
		loading = true;
	}
	
	synchronized void unlockForLoad() {
		assert loading;
		loading = false;
		notifyAll();
	}
	
}
