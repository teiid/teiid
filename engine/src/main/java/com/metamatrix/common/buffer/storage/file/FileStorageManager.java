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

package com.metamatrix.common.buffer.storage.file;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.buffer.BufferManagerPropertyNames;
import com.metamatrix.common.buffer.LobTupleBatch;
import com.metamatrix.common.buffer.StorageManager;
import com.metamatrix.common.buffer.TupleBatch;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.common.util.PropertiesUtils;
import com.metamatrix.core.log.MessageLevel;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.dqp.util.LogConstants;
import com.metamatrix.query.execution.QueryExecPlugin;
/**
 * This class stores batches in files on disk in a specified directory.  Every
 * tuple source gets it's own set of files, named b_<i>id</i>_<i>num</i> where <i>id</i> is the
 * unique id value of the TupleSourceID, and <i>num</i> is a file counter for the files
 * associated with the TupleSourceID.  Batches are stored random access into the
 * file, typically but not necessarily in order.  An in memory data structure stores
 * info about all open files and where the batches are in the file (file pointers and
 * lengths).
 */
public class FileStorageManager implements StorageManager {

    private static final String FILE_PREFIX = "b_"; //$NON-NLS-1$

    // Initialization
    private int maxOpenFiles = 10;
    private long maxFileSize = 2L * 1024L * 1024L * 1024L; // 2GB
    private String directory;
    private File dirFile;

    // State
    private Map tupleSourceMap = new HashMap();          // TupleSourceID -> TupleSourceInfo
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
     * @see com.metamatrix.common.buffer.BufferManagerPropertyNames#BUFFER_STORAGE_DIRECTORY
     * @see com.metamatrix.common.buffer.BufferManagerPropertyNames#MAX_OPEN_FILES
     * @see com.metamatrix.common.buffer.BufferManagerPropertyNames#MAX_FILE_SIZE
     */
    public void initialize(Properties props) throws MetaMatrixComponentException {
        this.directory = props.getProperty(BufferManagerPropertyNames.BUFFER_STORAGE_DIRECTORY);
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

        // Set up max number of open file descriptors
        maxOpenFiles = PropertiesUtils.getIntProperty(props, BufferManagerPropertyNames.MAX_OPEN_FILES, 10);
        
        // Set the max file size
        maxFileSize = PropertiesUtils.getIntProperty(props, BufferManagerPropertyNames.MAX_FILE_SIZE, 2048) * 1024L * 1024L; // Multiply by 1MB
    }

    /**
     * Return file type: {@link com.metamatrix.common.buffer.StorageManager.TYPE_FILE}
     * @return File type constant
     */
    public int getStorageType() {
        return StorageManager.TYPE_FILE;
    }

    /**
     * Look up tuple source info and possibly create.  First the file map is used to find an
     * existing file info.  If the info is found it is returned.  If not, then
     * a TupleSourceInfo is created according to shouldCreate flag
     * @param sourceID Source identifier
     * @param shouldCreate true if this method should create info related to the tuple source ID if it does not exist.
     * @return All the tuple source info or null if shouldCreate == false and no tuple source info was found
     */
    private TupleSourceInfo getTupleSourceInfo(TupleSourceID sourceID, boolean shouldCreate) {

        // Try to find in cache
        synchronized(tupleSourceMap) {
            TupleSourceInfo info = (TupleSourceInfo) tupleSourceMap.get(sourceID);
            if(info == null && shouldCreate) {
                info = new TupleSourceInfo();
                tupleSourceMap.put(sourceID, info);
            }
            return info;
        }
    }
    
    /**
     * Creates a new file to the specified TupleSourceID
     * @param sourceID The TupleSourceID
     * @param fileNumber a number uniquely identifying this file within the set of tuple source files.
     * @return the newly created file with the name b_<i>sourceID</i>_<i>fileNumber</i>
     * @throws MetaMatrixComponentException if a file with this name already exists in the directory, or if it cannot be created
     * @since 4.2
     */
    private File createFile(TupleSourceID sourceID, int fileNumber) throws MetaMatrixComponentException {
        File storageFile = new File(this.directory, FILE_PREFIX + sourceID.getIDValue() + "_" + fileNumber); //$NON-NLS-1$
        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_STORAGE_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_STORAGE_MGR, "Creating temporary storage area file " + storageFile.getAbsoluteFile()); //$NON-NLS-1$
        }
        try {
            boolean created = storageFile.createNewFile();
            if(!created) {
                throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("FileStoreageManager.file_exists", storageFile.getAbsoluteFile())); //$NON-NLS-1$                        
            }
        } catch(IOException e) {
        	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_creating", storageFile.getAbsoluteFile())); //$NON-NLS-1$
        }
        return storageFile;
    }

    /**
     * Add a batch to the storage manager.  Because we don't implement remove, it's
     * possible that this batch already exists, in which case nothing is done.
     * @param sourceID Source identifier
     * @param batch Batch to add
     */
    public void addBatch(TupleSourceID sourceID, TupleBatch batch, String[] types)
        throws MetaMatrixComponentException {

    	/* Right now we do not support the saving of the lobs to the disk.
         * by throwing an exception the memory is never released for lobs, which is same
         * as keeping them in a map.  This is not going to be memory hog because, the actual
         * lob (clob or blob) are backed by connector, xml is backed by already persisted 
         * tuple source. Here we are only saving the referenes to the actual objects.
         */
        if (batch instanceof LobTupleBatch) {
        	throw new MetaMatrixComponentException(QueryExecPlugin.Util.getString("FileStorageManager.can_not_save_lobs")); //$NON-NLS-1$
        }
        
        // Defect 13342 - addBatch method now creates spill files if the total bytes exceeds the max file size limit
        TupleSourceInfo tsInfo = getTupleSourceInfo(sourceID, true);
        synchronized (tsInfo) {
            if (tsInfo.isRemoved) {
                return;
            }
            Integer batchKey = new Integer(batch.getBeginRow());
            if (tsInfo.tupleBatchPointers != null && tsInfo.tupleBatchPointers.containsKey(batchKey)) {
                return;
            }
            byte[] bytes = convertToBytes(batch, types);
            if (bytes.length > maxFileSize) {
                LogManager.logWarning(LogConstants.CTX_STORAGE_MGR, "Detected an attempt to save a batch (" + sourceID + ", begin=" + batch.getBeginRow()+ ", size=" + bytes.length + ") larger than the buffer max file size setting of " + maxFileSize + " bytes. The buffer manager will ignore the max file size setting for this batch, and create a buffer file dedicated to this batch. It may be necessary to reduce the buffer batch setting or increase the buffer max file size setting.");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
            }
            FileInfo fileInfo = tsInfo.getMostRecentlyCreatedFile();
            if (fileInfo == null ||
                (/*fileInfo.file.length() != 0 && */fileInfo.file.length() + bytes.length > maxFileSize)) {
                // Create and add
                fileInfo = new FileInfo(createFile(sourceID, tsInfo.storageFiles.size()));
                tsInfo.storageFiles.add(fileInfo);
            }
            long pointer = 0;

            if (LogManager.isMessageToBeRecorded(LogConstants.CTX_STORAGE_MGR, MessageLevel.DETAIL)) {
                LogManager.logDetail(LogConstants.CTX_STORAGE_MGR, "Adding batch to storage area file " + fileInfo.file.getAbsoluteFile() + " [ sourceID: " + sourceID + "batch: " + batch + " ]"); //$NON-NLS-1$  //$NON-NLS-2$  //$NON-NLS-3$  //$NON-NLS-4$
            }
            try {
                // Get access to the file and remember whether we had to open it or not
                fileInfo.open();
                RandomAccessFile fileAccess = fileInfo.getAccess();

                // Store the batch in the file
                pointer = fileAccess.length();
                fileAccess.setLength(pointer + bytes.length);
                fileAccess.seek(pointer);
                fileAccess.write(bytes);
            } catch(IOException e) {
                throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", fileInfo.file.getAbsoluteFile())); //$NON-NLS-1$
            } finally {
                fileInfo.close();
            }

            // Update the pointers
            tsInfo.tupleBatchPointers.put(batchKey, new PointerInfo(fileInfo, pointer, bytes.length));
        }
    }

    /**
     * Convert from an object to a byte array
     * @param object Object to convert
     * @return Byte array
     */
    private byte[] convertToBytes(TupleBatch batch, String[] types) throws MetaMatrixComponentException {
        ObjectOutputStream oos = null;
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);

            batch.setDataTypes(types);
            batch.writeExternal(oos);
            oos.flush();
            return baos.toByteArray();

        } catch(IOException e) {
        	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStorageManager.batch_error")); //$NON-NLS-1$
        } finally {
            if(oos != null) {
                try {
                    oos.close();
                } catch(IOException e) {
                }
            }
        }
    }

    /**
     * Get a batch from the storage manager based on the beginRow.
     * @param sourceID Source identifier
     * @param beginRow Beginning row of batch to retrieve
     * @return Batch retrieved
     */
    public TupleBatch getBatch(TupleSourceID sourceID, int beginRow, String[] types)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        TupleSourceInfo info = getTupleSourceInfo(sourceID, false);
        if(info == null) {
        	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", sourceID)); //$NON-NLS-1$
        }

        byte[] bytes = null;
        synchronized(info) {
            if(info.isRemoved) {
            	throw new TupleSourceNotFoundException(QueryExecPlugin.Util.getString("BufferManagerImpl.tuple_source_not_found", sourceID)); //$NON-NLS-1$
            }
            // Find pointer
            PointerInfo pointerInfo = (PointerInfo) info.tupleBatchPointers.get(new Integer(beginRow));
            Assertion.isNotNull(pointerInfo);

            FileInfo fileInfo = pointerInfo.fileInfo;
            // Get access to the file
            RandomAccessFile fileAccess = null;
            try {
                fileInfo.open();
                fileAccess = fileInfo.getAccess();
                fileAccess.seek(pointerInfo.pointer);
                bytes = new byte[pointerInfo.length];
                fileAccess.readFully(bytes);
                ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bais);

                TupleBatch batch = new TupleBatch();
                batch.setDataTypes(types);
                batch.readExternal(ois);
                return batch;
            } catch(IOException e) {
            	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", fileInfo.file.getAbsoluteFile()));
            } catch (ClassNotFoundException e) {
            	throw new MetaMatrixComponentException(e, QueryExecPlugin.Util.getString("FileStoreageManager.error_reading", fileInfo.file.getAbsoluteFile()));
			} finally {
                fileInfo.close();
            }
        }
    }

    /**
     * This method does nothing - rather than deleting batches from the middle of a RandomAccessFile,
     * which would be very expensive, we just handle the possibility that a batch already exists
     * in the addBatch method.
     * @param sourceID Source identifier
     * @param beginRow Beginning batch row to remove
     */
    public void removeBatch(TupleSourceID sourceID, int beginRow)
        throws TupleSourceNotFoundException, MetaMatrixComponentException {

        // nothing - don't remove batches as it is too expensive
    }

    /**
     * Remove all batches for a sourceID.  Before removal, the file is closed.
     * @param sourceID Tuple source ID
     */
    public void removeBatches(TupleSourceID sourceID) throws MetaMatrixComponentException {
        TupleSourceInfo info = null;
        // Remove info from the file map
        synchronized(tupleSourceMap) {
            info = (TupleSourceInfo)tupleSourceMap.remove(sourceID);
        }

        // Didn't find a file
        if(info == null) {
            return;
        }

        if (LogManager.isMessageToBeRecorded(LogConstants.CTX_STORAGE_MGR, MessageLevel.DETAIL)) {
            LogManager.logDetail(LogConstants.CTX_STORAGE_MGR, "Removing storage for " + sourceID); //$NON-NLS-1$
        }

        synchronized(info) {
            if(info.isRemoved) {
                // Someone else got here first!
                return;
            }

            // If open, close the file and decrement the open file counter
            for (int i = 0; i < info.storageFiles.size(); i++) {
                FileInfo fileInfo = (FileInfo)info.storageFiles.get(i);
                fileInfo.delete();
            }
            // Delete the file and mark info as being removed
            info.isRemoved = true;
        }
    }

    /**
     * This method removes all storage area files by walking through the file info
     * map and closing and removing each file.
     */
    public synchronized void shutdown() {

	    LogManager.logDetail(LogConstants.CTX_STORAGE_MGR, "Removing all storage area files "); //$NON-NLS-1$

		Iterator tsIter = tupleSourceMap.keySet().iterator();

		while(tsIter.hasNext()) {
			TupleSourceID key = (TupleSourceID)tsIter.next();
            try {
                removeBatches(key);
            } catch (MetaMatrixComponentException e) {
                LogManager.logWarning(LogConstants.CTX_STORAGE_MGR, e, "Shutdown failed while removing batches for tuple source: " + key); //$NON-NLS-1$
            }
		}

        tupleSourceMap = null;
    }

    public int getOpenFiles() {
        return this.fileCache.size();
    }


    private class FileInfo {
    	private File file;
        private RandomAccessFile fileData;       // may be null if not open

        public FileInfo(File file) {
            this.file = file;
        }

        public boolean isOpen() {
            return fileData != null;
        }

        public void open() throws FileNotFoundException {
        	if(this.fileData == null) {
        		this.fileData = fileCache.remove(this.file);
        		if (this.fileData == null) {
                    this.fileData = new RandomAccessFile(file, "rw"); //$NON-NLS-1$
        		}
            }
        }

        public RandomAccessFile getAccess() {
            return this.fileData;
        }

        public void close() {
            if(this.fileData != null) {
            	fileCache.put(file, this.fileData);
            	this.fileData = null;
            }
        }
        
        public void delete()  {
            if (this.fileData == null) {
            	this.fileData = fileCache.remove(this.file);
            }
    		if (this.fileData != null) {
	        	try {
				    this.fileData.close();
				} catch(Exception e) {
				}
				this.fileData = null;
    		}
			file.delete();
        }

        public String toString() {
            return "FileInfo<" + file.getName() + ", has fileData = " + (fileData != null) + ">"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        }
    }

    private static class PointerInfo {
        FileInfo fileInfo;
        public long pointer;
        public int length;

        public PointerInfo(FileInfo fileInfo, long pointer, int length) {
            this.fileInfo = fileInfo;
            this.pointer = pointer;
            this.length = length;
        }
    }
    
    private static class TupleSourceInfo {
        Map tupleBatchPointers = new HashMap(); // beginRow -> PointerInfo
        List storageFiles = new ArrayList(2); // Stores all the FileInfos for this tupleSource
        private boolean isRemoved = false;
        
        FileInfo getMostRecentlyCreatedFile() {
            if (storageFiles.isEmpty()) {
                return null;
            }
            return (FileInfo)storageFiles.get(storageFiles.size() - 1);
        }
    }

}
