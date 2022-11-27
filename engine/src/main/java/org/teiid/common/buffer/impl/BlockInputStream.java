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

import org.teiid.common.buffer.ExtensibleBufferedInputStream;

/**
 * TODO: support freeing of datablocks as we go
 */
final class BlockInputStream extends ExtensibleBufferedInputStream {
    private final BlockManager manager;
    private final int maxBlock;
    int blockIndex;

    BlockInputStream(BlockManager manager, int blockCount) {
        this.manager = manager;
        this.maxBlock = blockCount;
    }

    @Override
    protected ByteBuffer nextBuffer() {
        if (maxBlock == blockIndex) {
            return null;
        }
        return manager.getBlock(blockIndex++);
    }

}