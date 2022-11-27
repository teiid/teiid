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
import java.nio.ByteBuffer;

public abstract class ExtensibleBufferedInputStream extends InputStream {
    ByteBuffer buf;

    @Override
    public int read() throws IOException {
        if (!ensureBytes()) {
            return -1;
        }
        return buf.get() & 0xff;
    }

    private boolean ensureBytes() throws IOException {
        if (buf == null || buf.remaining() == 0) {
            buf = nextBuffer();
            if (buf == null) {
                return false;
            }
        }
        return true;
    }

    protected abstract ByteBuffer nextBuffer() throws IOException;

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        if (!ensureBytes()) {
            return -1;
        }
        len = Math.min(len, buf.remaining());
        buf.get(b, off, len);
        return len;
    }

    @Override
    public void reset() throws IOException {
        if (buf != null) {
            buf.rewind();
        }
    }

    public ByteBuffer getBuffer() throws IOException {
        if (!ensureBytes()) {
            return null;
        }
        return buf;
    }

}