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

import java.util.List;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.Streamable;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.util.Options;


/**
 * The buffer manager controls how memory is used and how data flows through
 * the system.  It uses {@link StorageManager storage managers}
 * to retrieve data, store data, and
 * transfer data.  The buffer manager has algorithms that tell it when and
 * how to store data.  The buffer manager should also be aware of memory
 * management issues.
 */
public interface BufferManager extends StorageManager, TupleBufferCache {

    public enum TupleSourceType {
        /**
         * Indicates that a tuple source is use during query processing as a
         * temporary results.
         */
        PROCESSOR,
        /**
         * Indicates that a tuple source represents a query's final results.
         */
        FINAL
    }

    public enum BufferReserveMode {
        /**
         * Claim all of the buffers requested, even if they are not available, without waiting
         */
        FORCE,
        /**
         * Claim unused buffers up to the amount requested without waiting
         */
        NO_WAIT
    }

    public static int DEFAULT_PROCESSOR_BATCH_SIZE = 256;
    public static int DEFAULT_MAX_PROCESSING_KB = -1;
    public static int DEFAULT_RESERVE_BUFFER_KB = -1;

    /**
     * Get the batch size to use during query processing.
     * @return Batch size (# of rows)
     */
    int getProcessorBatchSize(List<? extends Expression> schema);

    /**
     * Get the nominal batch size target
     * @return
     */
    int getProcessorBatchSize();

    TupleBuffer createTupleBuffer(List elements, String groupName, TupleSourceType tupleSourceType)
    throws TeiidComponentException;

    /**
     * Return the max that can be temporarily held potentially
     * across even a blocked exception.
     * @return
     */
    int getMaxProcessingSize();

    /**
     * Creates a new {@link FileStore}.
     * @param name
     * @return
     */
    FileStore createFileStore(String name);

    /**
     * Reserve up to count buffers for use.
     * @param count
     * @param mode
     * @return
     */
    int reserveBuffers(int count, BufferReserveMode mode);

    /**
     * Releases the buffers reserved by a call to {@link BufferManager#reserveBuffers(int, BufferReserveMode)}
     * @param count
     */
    void releaseBuffers(int count);

    /**
     * Get the size estimate for the given schema.
     */
    int getSchemaSize(List<? extends Expression> elements);

    STree createSTree(List<? extends Expression> elements, String groupName, int keyLength);

    void addTupleBuffer(TupleBuffer tb);

    /**
     * Set the maxActivePlans as a hint at determining the maxProcessing
     * @param maxActivePlans
     */
    void setMaxActivePlans(int maxActivePlans);

    void setOptions(Options options);

    void persistLob(final Streamable<?> lob,
            final FileStore store, byte[] bytes) throws TeiidComponentException;

    int reserveBuffersBlocking(int count, long[] attempts, boolean force) throws BlockedException;

    void releaseOrphanedBuffers(long count);

    Options getOptions();
}
