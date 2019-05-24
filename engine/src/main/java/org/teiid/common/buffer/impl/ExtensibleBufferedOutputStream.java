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
import java.io.OutputStream;
import java.nio.ByteBuffer;

public abstract class ExtensibleBufferedOutputStream extends OutputStream {

    protected ByteBuffer buf;
    protected int bytesWritten;
    private int startPosition;

    public ExtensibleBufferedOutputStream() {
    }

    public void write(int b) throws IOException {
        ensureBuffer();
        buf.put((byte)b);
    }

    public void write(byte b) throws IOException {
        ensureBuffer();
        buf.put(b);
    }

    private void ensureBuffer() throws IOException {
        if (buf != null) {
            if (buf.remaining() != 0) {
                return;
            }
            flush();
        }
        buf = newBuffer();
        startPosition = buf.position();
    }

    public void write(byte b[], int off, int len) throws IOException {
        while (len > 0) {
            ensureBuffer();
            int toCopy = Math.min(buf.remaining(), len);
            buf.put(b, off, toCopy);
            len -= toCopy;
            off += toCopy;
            if (buf.remaining() > 0) {
                break;
            }
        }
    }

    public void flush() throws IOException {
        if (buf != null) {
            int bytes = buf.position() - startPosition;
            if (bytes > 0) {
                bytesWritten += flushDirect(bytes);
            }
        }
        buf = null;
    }

    protected abstract ByteBuffer newBuffer() throws IOException;

    /**
     * Flush up to i bytes where i is the current position of the buffer
     */
    protected abstract int flushDirect(int i) throws IOException;

    @Override
    public void close() throws IOException {
        flush();
    }

    public int getBytesWritten() {
        return bytesWritten;
    }

}
