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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import org.teiid.common.buffer.AutoCleanupUtil.Removable;
import org.teiid.core.types.DataTypeManager;

public abstract class FileStore implements Removable {

    /**
     * A customized buffered stream with an exposed buffer
     */
    public final class FileStoreOutputStream extends OutputStream {

        private byte[] buffer;
        private int count;
        private boolean bytesWritten;
        private boolean closed;
        private byte[] singleByte = new byte[1];
        private int maxSize;

        public FileStoreOutputStream(int size) {
            this.maxSize = size;
            this.buffer = new byte[Math.min(size, 1<<8)];
        }

        @Override
        public void write(int b) throws IOException {
            singleByte[0] = (byte)b;
            write(singleByte, 0, 1);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            checkOpen();
            if (len > maxSize) {
                flushBuffer();
                writeDirect(b, off, len);
                return;
            }
            if (!bytesWritten && buffer.length < maxSize && count + len > buffer.length) {
                byte[] nextBuffer = new byte[Math.min(maxSize, DataTypeManager.nextPowOf2(count + len))];
                System.arraycopy(buffer, 0, nextBuffer, 0, count);
                buffer = nextBuffer;
            }
            int bufferedLength = Math.min(len, buffer.length - count);
            if (count < buffer.length) {
                System.arraycopy(b, off, buffer, count, bufferedLength);
                count += bufferedLength;
                if (bufferedLength == len) {
                    return;
                }
            }
            flushBuffer();
            System.arraycopy(b, off + bufferedLength, buffer, count, len - bufferedLength);
            count += len - bufferedLength;
        }

        private void writeDirect(byte[] b, int off, int len) throws IOException {
            FileStore.this.write(b, off, len);
            bytesWritten = true;
        }

        public void flushBuffer() throws IOException {
            checkOpen();
            if (count > 0) {
                writeDirect(buffer, 0, count);
                count = 0;
            }
        }

        /**
         * Return the buffer.  Can be null if closed and the underlying filestore
         * has been written to.
         * @return
         */
        public byte[] getBuffer() {
            return buffer;
        }

        public int getCount() {
            return count;
        }

        public boolean bytesWritten() {
            return bytesWritten;
        }

        @Override
        public void flush() throws IOException {
            if (bytesWritten) {
                flushBuffer();
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            flush();
            closed = true;
            if (bytesWritten) {
                this.buffer = null;
            } else {
                //truncate
                this.buffer = Arrays.copyOf(this.buffer, this.count);
            }
        }

        private void checkOpen() {
            if (closed) {
                throw new IllegalStateException("Alread closed"); //$NON-NLS-1$
            }
        }

    }

    private AtomicBoolean removed = new AtomicBoolean();

    public abstract long getLength();

    public abstract void setLength(long length) throws IOException;

    public int read(long fileOffset, byte[] b, int offSet, int length)
            throws IOException {
        checkRemoved();
        return readWrite(fileOffset, b, offSet, length, false);
    }

    private void checkRemoved() throws IOException {
        if (removed.get()) {
            throw new IOException("already removed"); //$NON-NLS-1$
        }
    }

    protected abstract int readWrite(long fileOffset, byte[] b, int offSet, int length, boolean write)
            throws IOException;

    public void readFully(long fileOffset, byte[] b, int offSet, int length) throws IOException {
        if (length == 0) {
            return;
        }
        int n = 0;
        do {
            int count = this.read(fileOffset + n, b, offSet + n, length - n);
            if (count <= 0 && length > 0) {
                throw new IOException("not enough bytes available"); //$NON-NLS-1$
            }
            n += count;
        } while (n < length);
    }

    public synchronized void write(byte[] bytes, int offset, int length) throws IOException {
        write(getLength(), bytes, offset, length);
    }

    public void write(long start, byte[] bytes, int offset, int length) throws IOException {
        int n = 0;
        do {
            checkRemoved();
            int count = this.readWrite(start + n, bytes, offset + n, length - n, true);
            if (count <= 0 && length > 0) {
                throw new IOException("not enough bytes available"); //$NON-NLS-1$
            }
            n += count;
        } while (n < length);
    }

    public void remove() {
        if (removed.compareAndSet(false, true)) {
            this.removeDirect();
        }
    }

    protected abstract void removeDirect();

    public ExtensibleBufferedInputStream createInputStream(final long start, final long length) {
        return new ExtensibleBufferedInputStream() {
            private long offset = start;
            private long streamLength = length;
            private ByteBuffer bb = ByteBuffer.allocate(1<<13);

            @Override
            protected ByteBuffer nextBuffer() throws IOException {
                int len = bb.capacity();
                if (this.streamLength != -1 && len > this.streamLength) {
                    len = (int)this.streamLength;
                }
                if (this.streamLength == -1 || this.streamLength > 0) {
                    int bytes = FileStore.this.read(offset, bb.array(), 0, len);
                    if (bytes == -1) {
                        return null;
                    }
                    bb.rewind();
                    bb.limit(bytes);
                    this.offset += bytes;
                    if (this.streamLength != -1) {
                        this.streamLength -= bytes;
                    }
                    return bb;
                }
                return null;
            }
        };
    }

    public InputStream createInputStream(final long start) {
        return createInputStream(start, -1);
    }

    public OutputStream createOutputStream() {
        return new OutputStream() {

            @Override
            public void write(int b) throws IOException {
                throw new UnsupportedOperationException("buffered reading must be used"); //$NON-NLS-1$
            }

            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                FileStore.this.write(b, off, len);
            }
        };
    }

    public FileStoreOutputStream createOutputStream(int maxMemorySize) {
        return new FileStoreOutputStream(maxMemorySize);
    }

}