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
import java.nio.ByteBuffer;

final class BlockOutputStream extends
        ExtensibleBufferedOutputStream {
    private final BlockManager blockManager;
    int blockNum = -1;
    private final int maxBlocks;
    private final boolean allocate;

    static final IOException exceededMax = new IOException();

    /**
     * @param blockManager
     * @param maxBlocks a max of -1 indicates use existing blocks
     */
    BlockOutputStream(BlockManager blockManager, int maxBlocks) {
        this.blockManager = blockManager;
        this.allocate = maxBlocks != -1;
        this.maxBlocks = maxBlocks - 2; //convert to an index
    }

    @Override
    protected ByteBuffer newBuffer() throws IOException {
        if (!allocate) {
            return blockManager.getBlock(++blockNum);
        }
        if (blockNum > maxBlocks) {
            throw exceededMax;
        }
        return blockManager.allocateBlock(++blockNum);
    }

    @Override
    protected int flushDirect(int i) throws IOException {
        return i;
    }

    public void writeLong(long v) throws IOException {
        write((byte)(v >>> 56));
        write((byte)(v >>> 48));
        write((byte)(v >>> 40));
        write((byte)(v >>> 32));
        write((byte)(v >>> 24));
        write((byte)(v >>> 16));
        write((byte)(v >>> 8));
        write((byte)(v >>> 0));
    }
}