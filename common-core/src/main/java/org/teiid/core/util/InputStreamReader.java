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

package org.teiid.core.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;

import org.teiid.core.CorePlugin;

/**
 * Replacement for the standard {@link java.io.InputStreamReader},
 * which suffers from a <a href="http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4744247">bug</a> in sun.nio.cs.StreamDecoder
 */
public class InputStreamReader extends Reader {

    private CharsetDecoder cd;
    private ReadableByteChannel rbc;
    private ByteBuffer bb;
    private CharBuffer cb;
    private boolean done;
    private int bytesProcessed;

    public InputStreamReader(InputStream in, CharsetDecoder cd) {
        this(in, cd, ReaderInputStream.DEFAULT_BUFFER_SIZE);
    }

    public InputStreamReader(InputStream in, CharsetDecoder cd, int bufferSize) {
        this.cd = cd;
        this.rbc = Channels.newChannel(in);
        this.bb = ByteBuffer.allocate(bufferSize);
        this.cb = CharBuffer.allocate((int)(bufferSize * (double)cd.maxCharsPerByte()));
        this.cb.limit(0);
    }

    @Override
    public void close() throws IOException {
        rbc.close();
        cd.reset();
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        if ((off < 0) || (off > cbuf.length) || (len < 0) ||
            ((off + len) > cbuf.length) || ((off + len) < 0)) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }
        while (!done && !cb.hasRemaining()) {
            int read = 0;
            int pos = bb.position();
            while ((read = rbc.read(bb)) == 0) {
                //blocking read
            }
            bb.flip();
            cb.clear();
            CoderResult cr = cd.decode(bb, cb, read == -1);
            checkResult(cr);
            if (read == -1) {
                cr = cd.flush(cb);
                checkResult(cr);
                done = true;
            }
            bytesProcessed += bb.position() - pos;
            if (bb.position() != read + pos) {
                bb.compact();
            } else {
                bb.clear();
            }
            cb.flip();
        }
        len = Math.min(len, cb.remaining());
        if (len == 0 && done) {
            return -1;
        }
        cb.get(cbuf, off, len);
        return len;
    }

    private void checkResult(CoderResult cr) throws IOException {
        if (!cr.isUnderflow() && cr.isError()) {
            if (cr.isMalformed() || cr.isUnmappable()) {
                try {
                    cr.throwException();
                } catch (CharacterCodingException e) {
                    throw new IOException(CorePlugin.Util.gs(CorePlugin.Event.TEIID10082, cd.charset().displayName(), bytesProcessed + bb.position() + 1), e);
                }
            }
            cr.throwException();
        }
    }

}