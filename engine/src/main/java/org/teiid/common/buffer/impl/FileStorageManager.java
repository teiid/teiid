/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.common.buffer.impl;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.teiid.common.buffer.AutoCleanupUtil;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.logging.MessageLevel;
import org.teiid.query.QueryPlugin;


/**
 * Implements file storage that automatically splits large files and limits the number of open files.
 */
public class FileStorageManager implements StorageManager {

    private static final long MB = 1024L * 1024L;
    public static final int DEFAULT_MAX_OPEN_FILES = 64;
    public static final long DEFAULT_MAX_BUFFERSPACE = 5L * 1024L * MB; //5 GB
    private static final String FILE_PREFIX = "b_"; //$NON-NLS-1$

    private long maxBufferSpace = DEFAULT_MAX_BUFFERSPACE;
    private AtomicLong usedBufferSpace = new AtomicLong();
    private AtomicInteger fileCounter = new AtomicInteger();

    private AtomicLong sample = new AtomicLong();

    private AtomicInteger outOfDiskCount = new AtomicInteger();

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
                if (fileInfo == null) {
                    return -1;
                }
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
            try {
                RandomAccessFile fileAccess = fileInfo.open();
                long newLength = fileOffset + length;
                setLength(fileAccess, newLength, false);
                fileAccess.seek(fileOffset);
                fileAccess.write(b, offSet, length);
            } finally {
                fileInfo.close();
            }
            return length;
        }

        private void setLength(RandomAccessFile fileAccess, long newLength, boolean truncate)
                throws IOException {
            long currentLength = fileAccess.length();
            long bytesUsed = newLength - currentLength;
            if (bytesUsed == 0) {
                return;
            }
            if (bytesUsed < 0) {
                if (!truncate) {
                    return;
                }
            } else if (bytesUsed > MB) {
                //this is a weak check, concurrent access may push us over the max.  we are just trying to prevent large overage allocations
                long used = usedBufferSpace.get() + bytesUsed;
                if (used > maxBufferSpace) {
                    System.gc(); //attempt a last ditch effort to cleanup
                    AutoCleanupUtil.doCleanup(false);
                    used = usedBufferSpace.get() + bytesUsed;
                    if (used > maxBufferSpace) {
                        outOfDiskCount.getAndIncrement();
                        throw new OutOfDiskException(QueryPlugin.Util.getString("FileStoreageManager.space_exhausted", bytesUsed, used, maxBufferSpace)); //$NON-NLS-1$
                    }
                }
            }
            fileAccess.setLength(newLength);
            long used = usedBufferSpace.addAndGet(bytesUsed);
            if (LogManager.isMessageToBeRecorded(org.teiid.logging.LogConstants.CTX_BUFFER_MGR, MessageLevel.DETAIL) && (sample.getAndIncrement() % 100) == 0) {
                LogManager.logDetail(LogConstants.CTX_BUFFER_MGR, "sampling bytes used:", used); //$NON-NLS-1$
            }
            if (bytesUsed > 0 && used > maxBufferSpace) {
                System.gc(); //attempt a last ditch effort to cleanup
                AutoCleanupUtil.doCleanup(false);
                used = usedBufferSpace.get();
                if (used > maxBufferSpace) {
                    fileAccess.setLength(currentLength);
                    usedBufferSpace.addAndGet(-bytesUsed);
                    outOfDiskCount.getAndIncrement();
                    throw new OutOfDiskException(QueryPlugin.Util.getString("FileStoreageManager.space_exhausted", bytesUsed, used, maxBufferSpace)); //$NON-NLS-1$
                }
            }
        }

        @Override
        public synchronized void setLength(long length) throws IOException {
            if (fileInfo == null) {
                fileInfo = new FileInfo(createFile(name));
            }
            try {
                setLength(fileInfo.open(), length, true);
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
    //use subdirectories to hold the files since we may create a relatively unbounded amount of lob files and
    //fs performance will typically degrade if a single directory is too large
    private File[] subDirectories = new File[256];

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
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30040, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30040));
        }

        dirFile = new File(this.directory);
        makeDir(dirFile);
        for (int i = 0; i < subDirectories.length; i++) {
            subDirectories[i] = new File(this.directory, "b" +i); //$NON-NLS-1$
            makeDir(subDirectories[i]);
        }
    }

    private static void makeDir(File file) throws TeiidComponentException {
        if(file.exists()) {
            if(! file.isDirectory()) {
                 throw new TeiidComponentException(QueryPlugin.Event.TEIID30041, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30041, file.getAbsoluteFile()));
            }
        } else if(! file.mkdirs()) {
             throw new TeiidComponentException(QueryPlugin.Event.TEIID30042, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID30042, file.getAbsoluteFile()));
        }
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public void setStorageDirectory(String directory) {
        this.directory = directory;
    }

    File createFile(String name) throws IOException {
        //spray the files into separate different directories in a round robin fashion.
        File storageFile = File.createTempFile(FILE_PREFIX + name + "_", null, this.subDirectories[fileCounter.getAndIncrement()&(this.subDirectories.length-1)]); //$NON-NLS-1$
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

    public int getOutOfDiskErrorCount() {
        return outOfDiskCount.get();
    }

    /**
     * Set the max amount of buffer space in bytes
     * @param maxBufferSpace
     */
    public void setMaxBufferSpace(long maxBufferSpace) {
        this.maxBufferSpace = maxBufferSpace;
    }

    @Override
    public long getMaxStorageSpace() {
        return maxBufferSpace;
    }

}
