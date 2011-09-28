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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;


/**
 * Implements file storage that automatically splits large files and limits the number of open files.
 */
public class FileStorageManager implements StorageManager {
	
	public static final int DEFAULT_MAX_OPEN_FILES = 64;
	public static final long DEFAULT_MAX_BUFFERSPACE = 50L * 1024L * 1024L * 1024L;
	private static final String FILE_PREFIX = "b_"; //$NON-NLS-1$
	
	private long maxBufferSpace = DEFAULT_MAX_BUFFERSPACE;
	private AtomicLong usedBufferSpace = new AtomicLong();
	
	private class FileInfo {
    	private File file;
        private RandomAccessFile fileData;       // may be null if not open

        public FileInfo(File file) {
            this.file = file;
        }

        public RandomAccessFile open() throws FileNotFoundException {
        	if(this.fileData == null) {
        		this.fileData = fileCache.remove(this.file);
        		if (this.fileData == null) {
        			this.fileData = new RandomAccessFile(file, "rw"); //$NON-NLS-1$
        		}
            }
        	return this.fileData;
        }

        public void close() {
        	fileCache.put(this.file, this.fileData);
            this.fileData = null;
        }
        
        public void delete()  {
        	if (fileData == null) {
        		fileData = fileCache.remove(this.file);
        	}
        	if (fileData != null) {
        		try {
        			fileData.close();
				} catch (IOException e) {
				}
        	}
        	file.delete();
        }

        public String toString() {
            return "FileInfo<" + file.getName() + ", has fileData = " + (fileData != null) + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }
	
	public class DiskStore extends FileStore {
	    private String name;
		private FileInfo fileInfo; 
	    
	    public DiskStore(String name) {
			this.name = name;
		}
	    
	    @Override
	    public synchronized long getLength() {
	    	if (fileInfo == null) {
	    		return 0;
	    	}
	    	return fileInfo.file.length();
	    }
	    
	    @Override
	    protected synchronized int readWrite(long fileOffset, byte[] b, int offSet,
	    		int length, boolean write) throws IOException {
	    	if (!write) {
				try {
					RandomAccessFile fileAccess = fileInfo.open();
			        fileAccess.seek(fileOffset);
			        return fileAccess.read(b, offSet, length);
				} finally {
					fileInfo.close();
				}
	    	}
			if (fileInfo == null) {
				fileInfo = new FileInfo(createFile(name));
	        }
			long bytesUsed = 0;
	        try {
	        	RandomAccessFile fileAccess = fileInfo.open();
	            long newLength = fileOffset + length;
	            bytesUsed = newLength - fileAccess.length();
	            if (bytesUsed > 0) {
		    		long used = usedBufferSpace.addAndGet(bytesUsed);
					if (used > maxBufferSpace) {
						usedBufferSpace.addAndGet(-bytesUsed);
						//TODO: trigger a compaction before this is thrown
						throw new IOException(QueryPlugin.Util.getString("FileStoreageManager.space_exhausted", maxBufferSpace)); //$NON-NLS-1$
					}
	            	fileAccess.setLength(bytesUsed);
	            	bytesUsed = 0;
	            }
	            fileAccess.seek(fileOffset);
	            fileAccess.write(b, offSet, length);
	        } finally {
	        	if (bytesUsed > 0) {
	        		usedBufferSpace.addAndGet(-bytesUsed);
	        	}
	        	fileInfo.close();
	        }	    		
	    	return length;
	    }
	    
	    @Override
	    public synchronized void setLength(long length) throws IOException {
	    	if (fileInfo == null) {
				fileInfo = new FileInfo(createFile(name));
	        }
	    	try {
	    		fileInfo.open().setLength(length);
	    	} finally {
	    		fileInfo.close();
	    	}
	    }
		
	    @Override
		public synchronized void removeDirect() {
			usedBufferSpace.addAndGet(-getLength());
			if (fileInfo != null){
				fileInfo.delete();
			}
		}
	    
	}

    // Initialization
    private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    private String directory;
    private File dirFile;

    // State
    private Map<File, RandomAccessFile> fileCache = Collections.synchronizedMap(new LinkedHashMap<File, RandomAccessFile>() {
    	@Override
    	protected boolean removeEldestEntry(
    			java.util.Map.Entry<File, RandomAccessFile> eldest) {
    		if (this.size() > maxOpenFiles) {
    			try {
					eldest.getValue().close();
				} catch (IOException e) {
				}
    			return true;
    		}
    		return false;
    	}
    });
    
    /**
     * Initialize
     */
    public void initialize() throws TeiidComponentException {
        if(this.directory == null) {
        	throw new TeiidComponentException(QueryPlugin.Util.getString("FileStoreageManager.no_directory")); //$NON-NLS-1$
        }

        dirFile = new File(this.directory);
        if(dirFile.exists()) {
            if(! dirFile.isDirectory()) {
            	throw new TeiidComponentException(QueryPlugin.Util.getString("FileStoreageManager.not_a_directory", dirFile.getAbsoluteFile())); //$NON-NLS-1$

            }
        } else if(! dirFile.mkdirs()) {
        	throw new TeiidComponentException(QueryPlugin.Util.getString("FileStoreageManager.error_creating", dirFile.getAbsoluteFile())); //$NON-NLS-1$
        }
    }
    
    public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}
    
    public void setStorageDirectory(String directory) {
		this.directory = directory;
	}
    
    File createFile(String name) throws IOException {
    	File storageFile = File.createTempFile(FILE_PREFIX + name + "_", null, this.dirFile); //$NON-NLS-1$
        if (LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(org.teiid.logging.LogConstants.CTX_BUFFER_MGR, "Created temporary storage area file " + storageFile.getAbsoluteFile()); //$NON-NLS-1$
        }
        return storageFile;
    }
    
    public FileStore createFileStore(String name) {
    	return new DiskStore(name);
    }
    
    public String getDirectory() {
		return directory;
	}
    
    Map<File, RandomAccessFile> getFileCache() {
		return fileCache;
	}
    
    public int getOpenFiles() {
    	return this.fileCache.size();
    }
    
    /**
     * Get the used buffer space in bytes
     * @return
     */
    public long getUsedBufferSpace() {
		return usedBufferSpace.get();
	}
    
    /**
     * Set the max amount of buffer space in bytes
     * @param maxBufferSpace
     */
    public void setMaxBufferSpace(long maxBufferSpace) {
		this.maxBufferSpace = maxBufferSpace;
	}

}
