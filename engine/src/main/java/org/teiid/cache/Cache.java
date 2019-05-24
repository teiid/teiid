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

package org.teiid.cache;

import java.util.Set;

/**
 * Abstraction over cache providers
 */
public interface Cache<K, V>  {

   /**
    * Retrieves the value for the given Key
    *
    * @param key key under which value is to be retrieved.
    * @return returns data held under specified key in cache
    */
    V get(K key);

   /**
    * Associates the specified value with the specified key this cache.
    * If the cache previously contained a mapping for this key, the old value is replaced by the specified value.
    *
    * @param key   key with which the specified value is to be associated.
    * @param value value to be associated with the specified key.
    * @param ttl the time for this entry to live
    * @return previous value associated with specified key, or <code>null</code> if there was no mapping for key.
    *        A <code>null</code> return can also indicate that the key previously associated <code>null</code> with the specified key,
    *        if the implementation supports null values.
    */
    V put(K key, V value, Long ttl);

   /**
    * Removes the value for this key from a Cache.
    * Returns the value to which the Key previously associated , or
    * <code>null</code> if the Key contained no mapping.
    *
    * @param key key whose mapping is to be removed
    * @return previous value associated with specified Node's key
    */
    V remove(K key);

    /**
     * Size of the cache
     * @return number of items in this cache
     */
    int size();

    /**
     * Removes all the keys and their values from the Cache
     */
    void clear();

    /**
     * Name of the cache node
     * @return
     */
    String getName();

    /**
     * Return all the keys
     * @return
     */
    Set<K> keySet();

    /**
     * If the cache is transactional
     * @return
     */
    boolean isTransactional();

}
