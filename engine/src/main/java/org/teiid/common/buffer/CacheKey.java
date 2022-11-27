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

public class CacheKey implements Comparable<CacheKey> {

    final private Long id;
    final protected long lastAccess;
    final protected long orderingValue;

    public CacheKey(Long id, long lastAccess, long orderingValue) {
        this.id = id;
        this.lastAccess = lastAccess;
        this.orderingValue = orderingValue;
    }

    public Long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return id.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CacheKey)) {
            return false;
        }
        return this.id.equals(((CacheKey)obj).getId());
    }

    public long getLastAccess() {
        return lastAccess;
    }

    public long getOrderingValue() {
        return orderingValue;
    }

    @Override
    public int compareTo(CacheKey o) {
        int result = orderingValue < o.orderingValue ? -1 : (orderingValue == o.orderingValue ? 0 : 1);
        if (result == 0) {
            result = lastAccess < o.lastAccess ? -1 : (lastAccess == o.lastAccess ? 0 : 1);
            if (result == 0) {
                return id.compareTo(o.id);
            }
        }
        return result;
    }

}