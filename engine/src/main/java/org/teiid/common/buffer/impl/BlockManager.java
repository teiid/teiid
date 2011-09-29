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

import org.teiid.common.buffer.impl.FileStoreCache.BlockInfo;

/**
 * Represents an INode
 * 
 * Returned BlockInfo may be shared.  If shared there and no guarantees about position and mark.
 * in particular system/index blocks can be used by multiple threads relative methods should be
 * avoided, but may be used for exclusive write operations.
 * Otherwise the position will be 0.
 * 
 * Due to buffermanager locking, non-index data blocks can be assumed to be thread-safe. 
 */
public interface BlockManager {
	
	int getInode();
	
	BlockInfo allocateBlock(int index);
	
	BlockInfo getBlock(int index);
	
	void updateBlock(BlockInfo block);
	
	void freeBlock(int index);
	
	void free();

}
