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
import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;

public class SplittableStorageManager implements StorageManager {
	
	public static final long DEFAULT_MAX_FILESIZE = 2 * 1024l;
    private long maxFileSize = DEFAULT_MAX_FILESIZE * 1024l * 1024l; // 2GB
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
		private List<FileStore> storageFiles = new ArrayList<FileStore>();
		
		private volatile long len;
	    
	    public SplittableFileStore(String name) {
			this.name = name;
		}
	    
	    @Override
	    public long getLength() {
	    	return len;
	    }
	    
	    @Override
	    protected int readWrite(long fileOffset, byte[] b, int offSet,
	    		int length, boolean write) throws IOException {
	    	FileStore store = null;
	    	if (!write) {
	    		synchronized (this) {
		    		if (fileOffset + length > len) {
		    			throw new IOException("Invalid file position " + fileOffset + " length " + length); //$NON-NLS-1$ //$NON-NLS-2$
		    		}
		    		store = storageFiles.get((int)(fileOffset/maxFileSize));
	    		}
		    	return store.read(fileOffset%maxFileSize, b, offSet, length);
			}
	    	synchronized (this) {
		    	ensureLength(fileOffset + length);
	    		store = storageFiles.get((int)(fileOffset/maxFileSize));
			}
	    	long fileBegin = (int)(fileOffset%maxFileSize);
	    	length = Math.min(length, (int)Math.min(Integer.MAX_VALUE, maxFileSize - fileBegin));
			store.write(fileBegin, b, offSet, length);
			return length;
	    }

		private void ensureLength(long length) throws IOException {
			if (length <= len) {
				return;
			}
			int numFiles = (int)(length/maxFileSize);
			long lastFileSize = length%maxFileSize;
			if (lastFileSize > 0) {
				numFiles++;
			}
			for (int i = storageFiles.size(); i < numFiles; i++) {
				FileStore newFileInfo = storageManager.createFileStore(name + "_" + storageFiles.size()); //$NON-NLS-1$
				storageFiles.add(newFileInfo);
				if (lastFileSize == 0 || i != numFiles - 1) {
					newFileInfo.setLength(maxFileSize);
				}
			}
			if (lastFileSize > 0) {
				storageFiles.get(storageFiles.size() - 1).setLength(lastFileSize);
			}
			len = length;
		}

	    @Override
	    public synchronized void setLength(long length) throws IOException {
			if (length > len) {
				ensureLength(length);
			} else {
				int numFiles = (int)(length/maxFileSize);
				long lastFileSize = length%maxFileSize;
				if (lastFileSize > 0) {
					numFiles++;
				}
				int toRemove = storageFiles.size() - numFiles;
				for (int i = 0; i < toRemove; i++) {
					FileStore store = storageFiles.remove(storageFiles.size() -1);
					store.remove();
				}
				if (lastFileSize > 0) {
					storageFiles.get(storageFiles.size() - 1).setLength(lastFileSize);
				}
			}
			len = length;
	    }
	    
		public synchronized void removeDirect() {
			for (FileStore info : storageFiles) {
				info.remove();
			}
			storageFiles.clear();
		}
		
	}
	
    public long getMaxFileSize() {
		return maxFileSize;
	}
	
    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize * 1024l * 1024l;
	}
    
    public void setMaxFileSizeDirect(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
    }
    
    public StorageManager getStorageManager() {
		return storageManager;
	}

}
