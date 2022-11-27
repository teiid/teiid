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

package org.teiid.dqp.internal.process;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.query.util.CommandContext;

/**
 * An {@link InputStream} wrapper that saves the input on read and provides a {@link InputStreamFactory}.
 */
public final class SaveOnReadInputStream extends FilterInputStream {

    class SwitchingInputStream extends FilterInputStream {

        protected SwitchingInputStream() {
            super(SaveOnReadInputStream.this);
        }

        public void setIn(InputStream is) {
            this.in = is;
        }

    }

    private SwitchingInputStream sis = new SwitchingInputStream();
    private final FileStoreInputStreamFactory fsisf;
    private FileStoreOutputStream fsos;
    private boolean saved;
    private boolean read;
    private boolean returned;

    InputStreamFactory inputStreamFactory = new InputStreamFactory() {

        @Override
        public InputStream getInputStream() throws IOException {
            if (!saved) {
                if (!returned) {
                    returned = true;
                    return sis;
                }
                //save the rest of the stream
                SaveOnReadInputStream.this.fsos.flush();
                long start = SaveOnReadInputStream.this.fsisf.getLength();
                SaveOnReadInputStream.this.close(); //force the pending read
                InputStream is = SaveOnReadInputStream.this.fsisf.getInputStream(start, -1);
                sis.setIn(is);
            }
            return fsisf.getInputStream();
        }

        @Override
        public StorageMode getStorageMode() {
            if (!saved) {
                try {
                    getInputStream().close();
                } catch (IOException e) {
                    CommandContext cc = CommandContext.getThreadLocalContext();
                    if (cc != null) {
                        cc.addWarning(e);
                    }
                    LogManager.logInfo(LogConstants.CTX_DQP, e.getMessage());
                    return StorageMode.FREE;
                }
            }
            return fsisf.getStorageMode();
        }

        @Override
        public void setTemporary(boolean temp) {
            fsisf.setTemporary(temp);
        };
    };

    public SaveOnReadInputStream(InputStream in,
            FileStoreInputStreamFactory fsisf) {
        super(in);
        this.fsisf = fsisf;
        fsos = fsisf.getOuputStream();
    }

    @Override
    public int read() throws IOException {
        read = true;
        int i = super.read();
        read = false;
        if (i > 0) {
            fsos.write(i);
        } else {
            saved = true;
        }
        return i;
    }

    @Override
    public int read(byte[] b, int off, int len)
            throws IOException {
        read = true;
        int bytes = super.read(b, off, len);
        read = false;
        if (bytes > 0) {
            fsos.write(b, off, bytes);
        } else if (bytes == -1) {
            saved = true;
        }
        return bytes;
    }

    @Override
    public void close() throws IOException {
        try {
            if (!saved && !read) {
                byte[] bytes = new byte[1<<13];
                while (!saved) {
                    read(bytes, 0, bytes.length);
                }
            }
            fsos.close();
        } finally {
            if (!saved) {
                fsisf.free();
                saved = true;
            }
            super.close();
        }
    }

    public InputStreamFactory getInputStreamFactory() {
        return inputStreamFactory;
    }
}