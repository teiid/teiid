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

import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;

public class SplittableStorageManager implements StorageManager {
	
	public static final long DEFAULT_MAX_FILESIZE = 2L * 1024L;
    private long maxFileSize = DEFAULT_MAX_FILESIZE * 1024L * 1024L; // 2GB
	private StorageManager storageManager;
	
	public SplittableStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}
	
	@Override
	public FileStore createFileStore(String name) {
		return new SplittableFileStore(name);
	}
	
	@Override
	public void initialize() throws TeiidComponentException {
		storageManager.initialize();
	}
	
	public class SplittableFileStore extends FileStore {
	    private String name;
		private ConcurrentSkipListMap<Long, FileStore> storageFiles = new ConcurrentSkipListMap<Long, FileStore>(); 
	    
	    public SplittableFileStore(String name) {
			this.name = name;
		}
	    
	    public int readDirect(long fileOffset, byte[] b, int offSet, int length) throws TeiidComponentException {
	    	Map.Entry<Long, FileStore> entry = storageFiles.floorEntry(fileOffset);
	    	FileStore fileInfo = entry.getValue();
	    	return fileInfo.read(fileOffset - entry.getKey(), b, offSet, length);
	    }

		public void writeDirect(byte[] bytes, int offset, int length) throws TeiidComponentException {
			Map.Entry<Long, FileStore> entry = this.storageFiles.lastEntry();
			boolean createNew = false;
			FileStore fileInfo = null;
			long fileOffset = 0;
			if (entry == null) {
				createNew = true;
			} else {
				fileInfo = entry.getValue();
				fileOffset = entry.getKey();
				createNew = entry.getValue().getLength() + length > getMaxFileSize();
			}
			if (createNew) {
				FileStore newFileInfo = storageManager.createFileStore(name + "_" + storageFiles.size()); //$NON-NLS-1$
	            if (fileInfo != null) {
	            	fileOffset += fileInfo.getLength();
	            }
	            storageFiles.put(fileOffset, newFileInfo);
	            fileInfo = newFileInfo;
	        }
			fileInfo.write(bytes, offset, length);
		}
		
		public void removeDirect() {
			for (FileStore info : storageFiles.values()) {
				info.remove();
			}
		}
		
	}
	
    public long getMaxFileSize() {
		return maxFileSize;
	}
	
    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize * 1024L * 1024L;
	}
    
    void setMaxFileSizeDirect(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
    }
    
    public StorageManager getStorageManager() {
		return storageManager;
	}

}
