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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.FileStore;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;

/**
 * Implements file storage that automatically splits large files and limits the number of open files.
 */
public class FileStorageManager implements StorageManager {
	
    private static final int DEFAULT_MAX_OPEN_FILES = 256;
	private static final String FILE_PREFIX = "b_"; //$NON-NLS-1$
	
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
        	RandomAccessFile raf = fileCache.remove(this.file);
        	if (raf != null) {
        		try {
					raf.close();
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
		private TreeMap<Long, FileInfo> storageFiles = new TreeMap<Long, FileInfo>(); 
	    
	    public DiskStore(String name) {
			this.name = name;
		}
	    
	    public synchronized int readDirect(long fileOffset, byte[] b, int offSet, int length) throws MetaMatrixComponentException {
	    	Map.Entry<Long, FileInfo> entry = storageFiles.floorEntry(fileOffset);
			Assertion.isNotNull(entry);
			FileInfo fileInfo = entry.getValue();
			try {
				RandomAccessFile fileAccess = fileInfo.open();
		        fileAccess.seek(fileOffset - entry.getKey());
		        return fileAccess.read(b, offSet, length);
			} catch (IOException e) {
				throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", fileInfo.file.getAbsoluteFile())); //$NON-NLS-1$
			} finally {
				fileInfo.close();
			}
	    }

		public void writeDirect(byte[] bytes, int offset, int length) throws MetaMatrixComponentException {
			Map.Entry<Long, FileInfo> entry = this.storageFiles.lastEntry();
			boolean createNew = false;
			FileInfo fileInfo = null;
			long fileOffset = 0;
			if (entry == null) {
				createNew = true;
			} else {
				fileInfo = entry.getValue();
				fileOffset = entry.getKey();
				createNew = entry.getValue().file.length() + length > getMaxFileSize();
			}
			if (createNew) {
				FileInfo newFileInfo = new FileInfo(createFile(name, storageFiles.size()));
	            if (fileInfo != null) {
	            	fileOffset += fileInfo.file.length();
	            }
	            storageFiles.put(fileOffset, newFileInfo);
	            fileInfo = newFileInfo;
	        }
	        try {
	        	RandomAccessFile fileAccess = fileInfo.open();
	            long pointer = fileAccess.length();
	            fileAccess.setLength(pointer + length);
	            fileAccess.seek(pointer);
	            fileAccess.write(bytes, offset, length);
	        } catch(IOException e) {
	            throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", fileInfo.file.getAbsoluteFile())); //$NON-NLS-1$
	        } finally {
	        	fileInfo.close();
	        }
		}
		
		public synchronized void removeDirect() {
			for (FileInfo info : storageFiles.values()) {
				info.delete();
			}
		}
		
	}

    // Initialization
    private int maxOpenFiles = DEFAULT_MAX_OPEN_FILES;
    private long maxFileSize = 2L * 1024L * 1024L * 1024L; // 2GB
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
     * Initialize with properties
     * @param props Initialization properties
     * @see com.metamatrix.common.buffer.BufferManager#BUFFER_STORAGE_DIRECTORY
     * @see com.metamatrix.common.buffer.BufferManager#MAX_OPEN_FILES
     * @see com.metamatrix.common.buffer.BufferManager#MAX_FILE_SIZE
     */
    public void initialize(Properties props) throws MetaMatrixComponentException {
    	PropertiesUtils.setBeanProperties(this, props, "org.teiid.buffer"); //$NON-NLS-1$
        if(this.directory == null) {
        	throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("FileStoreageManager.no_directory")); //$NON-NLS-1$
        }

        dirFile = new File(this.directory);
        if(dirFile.exists()) {
            if(! dirFile.isDirectory()) {
            	throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("FileStoreageManager.not_a_directory", dirFile.getAbsoluteFile())); //$NON-NLS-1$

            }
        } else if(! dirFile.mkdirs()) {
        	throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("FileStoreageManager.error_creating", dirFile.getAbsoluteFile())); //$NON-NLS-1$
        }
    }
    
    public void setMaxFileSize(long maxFileSize) {
    	this.maxFileSize = maxFileSize * 1024L * 1024L;
	}
    
    void setMaxFileSizeDirect(long maxFileSize) {
    	this.maxFileSize = maxFileSize;
    }
    
    public void setMaxOpenFiles(int maxOpenFiles) {
		this.maxOpenFiles = maxOpenFiles;
	}
    
    public void setStorageDirectory(String directory) {
		this.directory = directory;
	}
    
    File createFile(String name, int fileNumber) throws MetaMatrixComponentException {
        try {
        	File storageFile = File.createTempFile(FILE_PREFIX + name + "_" + String.valueOf(fileNumber) + "_", null, this.dirFile); //$NON-NLS-1$ //$NON-NLS-2$
            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_STORAGE_MGR, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_STORAGE_MGR, "Created temporary storage area file " + storageFile.getAbsoluteFile()); //$NON-NLS-1$
            }
            return storageFile;
        } catch(IOException e) {
        	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_creating", name + "_" + fileNumber)); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }
    
    public FileStore createFileStore(String name) {
    	return new DiskStore(name);
    }
    
    public long getMaxFileSize() {
		return maxFileSize;
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

}
