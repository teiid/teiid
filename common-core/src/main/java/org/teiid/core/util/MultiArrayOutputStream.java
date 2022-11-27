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

import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * A dynamic buffer that limits copying overhead
 */
public class MultiArrayOutputStream extends OutputStream {

    private byte bufferIndex;
    private int index;
    private int count;
    private byte[][] bufs = new byte[15][];

    public MultiArrayOutputStream(int initialSize) {
        bufs[0] = new byte[initialSize];
    }

    public void reset(int newIndex) {
        Assertion.assertTrue(newIndex < bufs[0].length);
        while (bufferIndex > 0) {
            bufs[bufferIndex--] = null;
        }
        count = index = newIndex;
    }

    @Override
    public void write(int b) throws IOException {
        int newIndex = index + 1;
        byte[] buf = bufs[bufferIndex];
        if (newIndex > buf.length) {
            buf = bufs[++bufferIndex] = new byte[buf.length << 1];
            buf[0] = (byte)b;
            index = 1;
        } else {
            buf[index] = (byte)b;
            index = newIndex;
        }
        count++;
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        int newIndex = index + len;
        byte[] buf = bufs[bufferIndex];
        if (newIndex > buf.length) {
            int copyLen = Math.min(buf.length - index, len);
            if (copyLen > 0) {
                System.arraycopy(b, off, buf, index, copyLen);
            }
            int to = off + len;
            int nextIndex = len - copyLen;
            int diff = (buf.length << 1) - nextIndex;
            if (diff > 0) {
                to += diff;
            }
            bufs[++bufferIndex] = Arrays.copyOfRange(b, off + copyLen, to);
            index = nextIndex;
        } else {
            System.arraycopy(b, off, buf, index, len);
            index = newIndex;
        }
        count += len;
    }

    public void writeTo(DataOutput out) throws IOException {
        for (byte i = 0; i <= bufferIndex; i++) {
            byte[] b = bufs[i];
            out.write(b, 0, bufferIndex == i?index:b.length);
        }
    }

    public int getCount() {
        return count;
    }

    public byte[][] getBuffers() {
        return bufs;
    }

    public int getIndex() {
        return index;
    }

}
