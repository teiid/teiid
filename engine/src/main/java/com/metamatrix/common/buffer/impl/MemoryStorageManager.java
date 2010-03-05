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

package com.metamatrix.common.buffer.impl;

import java.nio.ByteBuffer;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.StorageManager;

public class MemoryStorageManager implements StorageManager {
    
    public void initialize() {
    }

	@Override
	public FileStore createFileStore(String name) {
		return new FileStore() {
			private ByteBuffer buffer = ByteBuffer.allocate(1 << 16);
			
			@Override
			public void writeDirect(byte[] bytes, int offset, int length) throws MetaMatrixComponentException {
				if (getLength() + length > buffer.capacity()) {
					ByteBuffer newBuffer = ByteBuffer.allocate(buffer.capacity() * 2 + length);
					newBuffer.put(buffer);
					buffer = newBuffer;
				}
				buffer.position((int)getLength());
				buffer.put(bytes, offset, length);
			}
			
			@Override
			public synchronized void removeDirect() {
				buffer = ByteBuffer.allocate(0);
			}
			
			@Override
			public synchronized int readDirect(long fileOffset, byte[] b, int offset, int length)
					throws MetaMatrixComponentException {
				if (fileOffset >= getLength()) {
					return -1;
				}
				int position = (int)fileOffset;
				buffer.position(position);
				length = Math.min(length, (int)getLength() - position);
				buffer.get(b, offset, length);
				return length;
			}
		};
	}
}