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

import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.lob.LobChunkProducer;
import org.teiid.core.CorePlugin;


/**
 * A wrapper class, given a InputStream object can convert a underlying
 * stream into sequence of ByteLobChunk objects of given chunk size.
 */
public class ByteLobChunkStream implements LobChunkProducer {
    private PushbackInputStream stream;
    private int chunkSize;
    private boolean closed;

    public ByteLobChunkStream(InputStream stream, int chunkSize) {
        this.stream = new PushbackInputStream(stream);
        this.chunkSize = chunkSize;
    }

    public LobChunk getNextChunk() throws IOException{

        if (this.closed) {
            throw new IllegalStateException(CorePlugin.Util.getString("stream_closed")); //$NON-NLS-1$
        }

        // read contents from the stream
        byte[] cbuf = new byte[this.chunkSize];
        int start = 0;
        int read = 0;
        while (true) {
            int currentRead = this.stream.read(cbuf, start, cbuf.length - start);
            if (currentRead == -1) {
                if (start == 0) {
                    return new LobChunk(new byte[0], true);
                }
                break;
            }
            read += currentRead;
            if (read < this.chunkSize) {
                start = read;
            } else {
                break;
            }
        }
        boolean isLast = false;
        if (read != this.chunkSize) {
            byte[] buf = new byte[read];
            System.arraycopy(cbuf, 0, buf, 0, read);
            cbuf = buf;
        }
        int next = this.stream.read();
        if (next == -1) {
            isLast = true;
        } else {
            this.stream.unread(next);
        }
        return new LobChunk(cbuf, isLast);
    }

    public void close() throws IOException {
        this.closed = true;
        this.stream.close();
    }
}