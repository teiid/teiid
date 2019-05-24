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

/**
 *
 */
package org.teiid.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

import org.teiid.core.CorePlugin;

/**
 * Implements a buffered {@link InputStream} for a given {@link Reader} and {@link Charset}
 */
public class ReaderInputStream extends InputStream {

    //even though we're dealing with chars, we'll use the same default
    static final int DEFAULT_BUFFER_SIZE = 1<<13;

    private final Reader reader;
    private CharBuffer cb;
    private ByteBuffer bb;
    private boolean done;
    private boolean wasOverflow;
    private CharsetEncoder encoder;
    private byte[] singleByte = new byte[1];

    /**
     * Creates a new inputstream that will replace any malformed/unmappable input
     * @param reader
     * @param charset
     */
    public ReaderInputStream(Reader reader, Charset charset) {
        this(reader, charset.newEncoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE),
                DEFAULT_BUFFER_SIZE);
    }

    public ReaderInputStream(Reader reader, CharsetEncoder encoder) {
        this(reader, encoder, DEFAULT_BUFFER_SIZE);
    }

    public ReaderInputStream(Reader reader, CharsetEncoder encoder, int bufferSize) {
        this.reader = reader;
        this.encoder = encoder;
        this.encoder.reset();
        this.cb = CharBuffer.allocate(bufferSize);
        this.bb = ByteBuffer.allocate(bufferSize);
        this.bb.limit(0);
    }

    @Override
    public int read(byte[] bbuf, int off, int len) throws IOException {
        if ((off < 0) || (off > bbuf.length) || (len < 0) ||
            ((off + len) > bbuf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        while (!done && !bb.hasRemaining()) {
            int read = 0;
            int pos = cb.position();
            if (!wasOverflow) {
                while ((read = reader.read(cb)) == 0) {
                    //blocking read
                }
                cb.flip();
            }
            bb.clear();
            CoderResult cr = encoder.encode(cb, bb, read == -1);
            checkResult(cr);
            if (read == -1 && !wasOverflow) {
                cr = encoder.flush(bb);
                checkResult(cr);
                if (!wasOverflow) {
                    done = true;
                }
            }
            if (!wasOverflow) {
                if (read != 0 && cb.position() != read + pos) {
                    cb.compact();
                } else {
                    cb.clear();
                }
            }
            bb.flip();
        }
        len = Math.min(len, bb.remaining());
        if (len == 0 && done) {
            return -1;
        }
        bb.get(bbuf, off, len);
        return len;
    }

    private void checkResult(CoderResult cr) throws IOException {
        if (cr.isOverflow()) {
            wasOverflow = true;
            assert bb.position() > 0;
        } else if (!cr.isUnderflow()) {
            try {
                cr.throwException();
            } catch (CharacterCodingException e) {
                throw new IOException(CorePlugin.Util.gs(CorePlugin.Event.TEIID10083, encoder.charset().displayName()), e);
            }
        } else {
            wasOverflow = false;
        }
    }

    @Override
    public int read() throws IOException {
        int read = read(singleByte, 0, 1);
        if (read == 1) {
            return singleByte[0] & 0xff;
        }
        assert read != 0;
        return -1;
    }

    @Override
    public void close() throws IOException {
        this.reader.close();
    }
}