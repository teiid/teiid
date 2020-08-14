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

package org.teiid.file;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;

public interface VirtualFile {

    /**
     * Get the file name
     */
    String getName();

    boolean isDirectory();

    /**
     * The full virtual path to the file including file name
     * It is not well-defined if this must start with a leading /
     * @return
     */
    String getPath();

    /**
     * The {@link InputStreamFactory} for utilizing this file as a blob or clob in the
     * engine.
     */
    default InputStreamFactory createInputStreamFactory() {
        InputStreamFactory isf = new InputStreamFactory () {
            @Override
            public InputStream getInputStream() throws IOException {
                return openInputStream(true);
            }

            @Override
            public StorageMode getStorageMode() {
                return VirtualFile.this.getStorageMode();
            }
        };

        isf.setSystemId(getName());
        isf.setLength(getSize());
        return isf;
    }

    /**
     * Get the {@link StorageMode} of this file.  Used by {@link #createInputStreamFactory()}. Defaults to OTHER (not local on disk nor memory).
     * @return
     */
    default StorageMode getStorageMode() {
        return StorageMode.OTHER;
    }

    /**
     * Open a stream for reading
     * @param lock true if a lock is requested.
     * It's up to the implementation whether to actually honor the lock.
     */
    InputStream openInputStream(boolean lock) throws IOException;

    /**
     * Open a stream for writing
     * @param lock true if a lock is requested.
     * It's up to the implementation whether to actually honor the lock.
     */
    OutputStream openOutputStream(boolean lock) throws IOException;

    /**
     * The last modified time in UTC milliseconds
     */
    long getLastModified();

    /**
     * The creation time in UTC milliseconds
     */
    long getCreationTime();

    /**
     * The size in bytes.
     * @return the size or -1 if unknown.
     */
    long getSize();

}
