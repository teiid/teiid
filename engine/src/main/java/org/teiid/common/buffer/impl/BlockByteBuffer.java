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

import java.nio.ByteBuffer;

/**
 * Provides buffer slices or blocks off of a central
 * set of buffers.
 */
public class BlockByteBuffer {

    private static class ByteBufferHolder {
        int size;
        volatile ByteBuffer buffer;

        public ByteBufferHolder(int size) {
            this.size = size;
        }

        public ByteBuffer duplicate(boolean direct) {
            if (buffer == null) {
                synchronized (this) {
                    if (buffer == null) {
                        this.buffer = allocate(size, direct);
                    }
                }
            }
            return buffer.duplicate();
        }
    }

    private static class BlockByteBufferData {
        int blockAddressBits;
        int segmentAddressBits;
        int segmentSize;
        int blockSize;
        int blockCount;
        boolean direct;
        ByteBufferHolder[] origBuffers;
    }

    private BlockByteBufferData data;
    private ByteBuffer[] buffers;

    /**
     * Creates a new {@link BlockByteBuffer} where each buffer segment will be
     * 1 &lt;&lt; segmentAddressBits (max of 30), and a total size of (1 &lt;&lt; blockAddressBits)&#42;blockCount.
     * @param segmentAddressBits
     * @param blockCount
     * @param blockAddressBits
     * @param direct
     */
    public BlockByteBuffer(int segmentAddressBits, int blockCount, int blockAddressBits, boolean direct) {
        this.data = new BlockByteBufferData();
        this.data.segmentAddressBits = segmentAddressBits;
        this.data.blockAddressBits = blockAddressBits;
        this.data.blockSize = 1 << blockAddressBits;
        this.data.segmentSize = 1 << this.data.segmentAddressBits;
        this.data.blockCount = blockCount;
        long size = ((long)blockCount)<<blockAddressBits;
        int fullSegments = (int)(size>>segmentAddressBits);
        int lastSegmentSize = (int) (size&(data.segmentSize-1));
        int segments = fullSegments;
        if (lastSegmentSize > 0) {
            segments++;
        }
        this.data.origBuffers = new ByteBufferHolder[segments];
        this.data.direct = direct;
        buffers = new ByteBuffer[segments];
        for (int i = 0; i < fullSegments; i++) {
            this.data.origBuffers[i] = new ByteBufferHolder(data.segmentSize);
        }
        if (lastSegmentSize > 0) {
            this.data.origBuffers[fullSegments] = new ByteBufferHolder(lastSegmentSize);
        }
    }

    public ByteBuffer[] getBuffers() {
        return buffers;
    }

    private BlockByteBuffer() {

    }

    public static ByteBuffer allocate(int size, boolean direct) {
        if (direct) {
            return ByteBuffer.allocateDirect(size);
        }
        return ByteBuffer.allocate(size);
    }

    public BlockByteBuffer duplicate() {
        BlockByteBuffer dup = new BlockByteBuffer();
        dup.data = data;
        dup.buffers = new ByteBuffer[buffers.length];
        return dup;
    }

    /**
     * Return a buffer positioned at the given start byte.
     * It is assumed that the caller will handle blocks in
     * a thread safe manner.
     * @param block
     * @return
     */
    public ByteBuffer getByteBuffer(int block) {
        if (block < 0 || block >= data.blockCount) {
            throw new IndexOutOfBoundsException("Invalid block " + block); //$NON-NLS-1$
        }
        int segment = block>>(data.segmentAddressBits-data.blockAddressBits);
        ByteBuffer bb = buffers[segment];
        if (bb == null) {
            bb = buffers[segment] = data.origBuffers[segment].duplicate(data.direct);
        } else {
            bb.rewind();
        }
        int position = (block<<data.blockAddressBits)&(data.segmentSize-1);
        bb.limit(position + data.blockSize);
        bb.position(position);
        return bb;
    }

}
