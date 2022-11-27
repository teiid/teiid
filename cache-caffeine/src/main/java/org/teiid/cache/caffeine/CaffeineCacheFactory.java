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

package org.teiid.cache.caffeine;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.teiid.cache.Cache;
import org.teiid.cache.CacheFactory;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;

@SuppressWarnings({"rawtypes", "unchecked"})
public class CaffeineCacheFactory implements CacheFactory {

    static class ExpiringValue<V> {
        private V value;
        private Long ttl;

        public ExpiringValue(V value, Long ttl) {
            this.value = value;
            this.ttl = ttl;
        }

        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (obj instanceof ExpiringValue) {
                return value.equals(((ExpiringValue)obj).value);
            }
            return value.equals(obj);
        }
    }

    static class CaffeineCache<K, V> implements Cache<K, V> {
        private String name;
        private Map<K, ExpiringValue<V>> delegate;

        CaffeineCache(String cacheName, int maxSize) {
            this.name = cacheName;
            this.delegate = Caffeine.newBuilder()
                    .maximumSize(maxSize)
                    .expireAfter(new Expiry<K, ExpiringValue<V>>() {
                        @Override
                        public long expireAfterCreate(@NonNull K key, @NonNull ExpiringValue<V> value, long currentTime) {
                            if (value.ttl == null) {
                                return Long.MAX_VALUE;
                            }
                            return TimeUnit.MILLISECONDS.toNanos(value.ttl);
                        }

                        @Override
                        public long expireAfterRead(@NonNull K key, @NonNull ExpiringValue<V> value, long currentTime,
                                @NonNegative long currentDuration) {
                            return currentDuration;
                        }

                        @Override
                        public long expireAfterUpdate(@NonNull K key, @NonNull ExpiringValue<V> value, long currentTime,
                                @NonNegative long currentDuration) {
                            if (value.ttl != null) {
                                long nanos = TimeUnit.MILLISECONDS.toNanos(value.ttl);
                                if (nanos < currentDuration) {
                                    return nanos;
                                }
                            }
                            return currentDuration;
                        }
                    })
                    .build().asMap();
        }

        @Override
        public V put(K key, V value, Long ttl) {
            ExpiringValue<V> existing = delegate.put(key, new ExpiringValue<>(value, ttl));
            if (existing != null) {
                return existing.value;
            }
            return null;
        }

        @Override
        public String getName() {
            return this.name;
        }

        @Override
        public boolean isTransactional() {
            return false;
        }

        @Override
        public V get(K key) {
            ExpiringValue<V> value = delegate.get(key);
            if (value != null) {
                return value.value;
            }
            return null;
        }

        @Override
        public V remove(K key) {
            ExpiringValue<V> v = delegate.remove(key);
            if (v != null) {
                return v.value;
            }
            return null;
        }

        @Override
        public int size() {
            return Math.toIntExact(delegate.size());
        }

        @Override
        public void clear() {
            delegate.clear();
        }

        @Override
        public Set<K> keySet() {
            return delegate.keySet();
        }
    }

    private Map<String, Cache> map = new HashMap<>();

    @Override
    public <K, V> Cache<K, V> get(String name) {
        map.put(name, new CaffeineCache<K,V>(name, 512));
        return map.get(name);
    }

    @Override
    public void destroy() {
        Set<String> keys = new HashSet<>(map.keySet());
        keys.forEach(k -> map.get(k).clear());
        map.clear();
    }
}