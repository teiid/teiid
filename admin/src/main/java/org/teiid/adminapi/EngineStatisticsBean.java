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

package org.teiid.adminapi;

public interface EngineStatisticsBean {

    /**
     * Active Number of Sessions in the engine
     * @return
     */
    int getSessionCount();

    /**
     * Total amount memory used by buffer manager for active queries and cached queries
     * @return
     * @see #getBufferHeapInUseKb()
     */
    @Deprecated
    long getTotalMemoryUsedInKB();

    /**
     * Heap memory estimate in use by buffer manager
     * @return
     */
    long getBufferHeapInUseKb();

    /**
     * Total memory used by buffer manager for active plans
     * @return
     * @see #getBufferHeapReservedByActivePlansKb()
     */
    @Deprecated
    long getMemoryUsedByActivePlansInKB();

    /**
     * Total processing memory reserved by active plans
     * @return
     */
    long getBufferHeapReservedByActivePlansKb();

    /**
     * Number of writes to disk by buffer manager to save the overflow from memory
     * @return
     */
    long getDiskWriteCount();

    /**
     * Number reads from the disk used by buffer manager that cache overflowed.
     * @return
     */
    long getDiskReadCount();

    /**
     * Total number of cache reads, includes disk and soft-cache references
     * @return
     * @see #getStorageReadCount()
     */
    @Deprecated
    long getCacheReadCount();

    /**
     * Total number of storage reads from the layer below the heap
     * @return
     */
    long getStorageReadCount();

    /**
     * Total number of cache writes, includes disk and soft-cache references
     * @return
     * @see #getStorageReadCount()
     */
    @Deprecated
    long getCacheWriteCount();

    /**
     * Total number of storage writes to the layer below the heap
     * @return
     */
    long getStorageWriteCount();

    /**
     * Disk space used by buffer manager to save overflowed memory contents
     * @return
     */
    long getDiskSpaceUsedInMB();

    /**
     * Current active plan count
     * @return
     */
    int getActivePlanCount();

    /**
     * Current number of waiting plans in the queue
     * @return
     */
    int getWaitPlanCount();

    /**
     * High water mark for the waiting plans
     * @return
     */
    int getMaxWaitPlanWaterMark();

}
