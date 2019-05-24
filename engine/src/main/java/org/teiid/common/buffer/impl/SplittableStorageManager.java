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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.StorageManager;
import org.teiid.core.TeiidComponentException;

/**
 * A storage manager that combines smaller files into a larger
 * logical file.
 *
 * The buffer methods assume that buffers cannot go beyond single
 * file boundaries.
 */
public class SplittableStorageManager implements StorageManager {

    public static final long DEFAULT_MAX_FILESIZE = 2 * 1024L;
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
                    if (fileOffset > len) {
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
            long fileBegin = fileOffset%maxFileSize;
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
            for (int i = storageFiles.size() - 1; i >= 0; i--) {
                this.storageFiles.remove(i).remove();
            }
        }

    }

    public long getMaxFileSize() {
        return maxFileSize;
    }

    public void setMaxFileSize(long maxFileSize) {
        this.maxFileSize = maxFileSize * 1024L * 1024L;
    }

    public void setMaxFileSizeDirect(long maxFileSize) {
        this.maxFileSize = maxFileSize;
    }

    public StorageManager getStorageManager() {
        return storageManager;
    }

    @Override
    public long getMaxStorageSpace() {
        return storageManager.getMaxStorageSpace();
    }

}
