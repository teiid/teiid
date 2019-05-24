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

public class CacheEntry extends BaseCacheEntry {
    private boolean persistent;
    private Object object;
    private final int sizeEstimate;
    private WeakReference<? extends Serializer<?>> serializer;

    public CacheEntry(Long oid) {
        this(new CacheKey(oid, 0, 0), 0, null, null, false);
    }

    public CacheEntry(CacheKey key, int sizeEstimate, Object object, WeakReference<? extends Serializer<?>> serializer, boolean persistent) {
        super(key);
        this.sizeEstimate = sizeEstimate;
        this.object = object;
        this.serializer = serializer;
        this.persistent = persistent;
    }

    public void setObject(Object object) {
        this.object = object;
    }

    public int getSizeEstimate() {
        return sizeEstimate;
    }

    public Object nullOut() {
        Object result = getObject();
        this.object = null;
        this.serializer = null;
        return result;
    }

    public Object getObject() {
        return object;
    }

    public void setPersistent(boolean persistent) {
        this.persistent = persistent;
    }

    public boolean isPersistent() {
        return persistent;
    }

    public void setSerializer(WeakReference<? extends Serializer<?>> serializer) {
        this.serializer = serializer;
    }

    public Serializer<?> getSerializer() {
        WeakReference<? extends Serializer<?>> ref = this.serializer;
        if (ref == null) {
            return null;
        }
        return ref.get();
    }

}