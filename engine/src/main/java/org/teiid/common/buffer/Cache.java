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

import java.lang.ref.WeakReference;
import java.util.Collection;

import org.teiid.core.TeiidComponentException;

/**
 * Represents the storage strategy for the {@link BufferManager}
 */
public interface Cache<T> extends StorageManager {
    /**
     * Must be called prior to adding any group entries
     * @param gid
     */
    void createCacheGroup(Long gid);

    /**
     * Remove an entire cache group
     *
     * TODO: this should use a callback on the buffermangaer to remove memory entries
     * without materializing all group keys
     * @param gid
     * @return
     */
    Collection<Long> removeCacheGroup(Long gid);

    /**
     * Must be called prior to adding an entry
     * @param gid
     * @param oid
     * @return if the add was successful
     */
    boolean addToCacheGroup(Long gid, Long oid);

    /**
     * Lock the object for load and return an identifier/lock
     * that can be used to retrieve the object.
     * @param oid
     * @param serializer
     * @return the identifier, may be null
     */
    T lockForLoad(Long oid, Serializer<?> serializer);

    /**
     * Must be called after lockForLoad
     */
    void unlockForLoad(T lock);

    /**
     * Get method, must be called using the object obtained in the
     * lockForLoad method
     * @return
     * @throws TeiidComponentException
     */
    CacheEntry get(T lock, Long oid, WeakReference<? extends Serializer<?>> ref) throws TeiidComponentException;

    /**
     * Adds an entry to the cache.
     * @param entry
     * @param s
     * @throws Exception
     */
    boolean add(CacheEntry entry, Serializer<?> s) throws Exception;

    /**
     * Remove an entry from the cache, return the sizeEstimate if the entry existed
     * @param gid
     * @param id
     */
    Integer remove(Long gid, Long id);

    void shutdown();

    long getMemoryBufferSpace();

    int getCacheGroupCount();

}