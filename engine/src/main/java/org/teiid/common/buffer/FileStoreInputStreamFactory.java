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

package org.teiid.common.buffer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.lang.ref.PhantomReference;
import java.nio.charset.Charset;

import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;

public final class FileStoreInputStreamFactory extends InputStreamFactory {
    private final FileStore lobBuffer;
    private FileStoreOutputStream fsos;
    private String encoding;
    private PhantomReference<Object> cleanup;
    private Writer writer;
    private boolean temporary;

    public FileStoreInputStreamFactory(FileStore lobBuffer, String encoding) {
        this.encoding = encoding;
        this.lobBuffer = lobBuffer;
        cleanup = AutoCleanupUtil.setCleanupReference(this, lobBuffer);
    }

    @Override
    public InputStream getInputStream() {
        return getInputStream(0, -1);
    }

    public InputStream getInputStream(long start, long len) {
        if (fsos != null && !fsos.bytesWritten()) {
            if (start > Integer.MAX_VALUE) {
                throw new AssertionError("Invalid start " + start); //$NON-NLS-1$
            }
            int s = (int)start;
            int intLen = fsos.getCount() - s;
            if (len >= 0) {
                intLen = (int)Math.min(len, len);
            }
            return new ByteArrayInputStream(fsos.getBuffer(), s, intLen);
        }
        return lobBuffer.createInputStream(start, len);
    }

    public byte[] getMemoryBytes() {
        if (fsos != null && !fsos.bytesWritten() && fsos.getBuffer().length == fsos.getCount()) {
            return fsos.getBuffer();
        }
        throw new IllegalStateException("In persistent mode or not closed for writing"); //$NON-NLS-1$
    }

    @Override
    public Reader getCharacterStream() throws IOException {
        return new InputStreamReader(getInputStream(), Charset.forName(encoding).newDecoder());
    }

    @Override
    public long getLength() {
        if (fsos != null && !fsos.bytesWritten()) {
            return fsos.getCount();
        }
        return lobBuffer.getLength();
    }

    /**
     * Returns a new writer instance that is backed by the shared output stream.
     * Closing a writer will prevent further writes.
     * @return
     */
    public Writer getWriter() {
        if (writer == null) {
            writer = new OutputStreamWriter(getOuputStream(), Charset.forName(encoding));
        }
        return writer;
    }

    /**
     * The returned output stream is shared among all uses.
     * Once closed no further writing can occur
     * @return
     */
    public FileStoreOutputStream getOuputStream() {
        return getOuputStream(DataTypeManager.MAX_LOB_MEMORY_BYTES);
    }

    /**
     * The returned output stream is shared among all uses.
     * Once closed no further writing can occur
     * @return
     */
    public FileStoreOutputStream getOuputStream(int maxMemorySize) {
        if (fsos == null) {
            fsos = lobBuffer.createOutputStream(maxMemorySize);
        }
        return fsos;
    }

    @Override
    public void free() {
        fsos = null;
        lobBuffer.remove();
        AutoCleanupUtil.removeCleanupReference(cleanup);
        cleanup = null;
    }

    @Override
    public StorageMode getStorageMode() {
        if (fsos == null || fsos.bytesWritten()) {
            return StorageMode.PERSISTENT;
        }
        return StorageMode.MEMORY;
    }

    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public void setTemporary(boolean b) {
        this.temporary = b;
    }
}