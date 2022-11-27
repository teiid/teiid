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

package org.teiid.translator;

import java.io.Serializable;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;

public class CacheDirective implements Serializable {

    public enum Scope {
        NONE,
        SESSION,
        USER,
        VDB
    }

    public enum Invalidation {
        /**
         * No invalidation - the default
         */
        NONE,
        /**
         * Invalidate after new results have been obtained
         */
        LAZY,
        /**
         * Invalidate immediately
         */
        IMMEDIATE
    }

    private static final long serialVersionUID = -4119606289701982511L;

    private Boolean prefersMemory;
    private Boolean updatable;
    private Boolean readAll;
    private Long ttl;
    private Scope scope;
    private Invalidation invalidation = Invalidation.NONE;

    public CacheDirective() {
    }

    public CacheDirective(Boolean prefersMemory, Long ttl) {
        this.prefersMemory = prefersMemory;
        this.ttl = ttl;
    }

    public Boolean getPrefersMemory() {
        return prefersMemory;
    }

    public void setPrefersMemory(Boolean prefersMemory) {
        this.prefersMemory = prefersMemory;
    }

    /**
     * Get the time to live in milliseconds
     * @return
     */
    public Long getTtl() {
        return ttl;
    }

    /**
     * Set the time to live in milliseconds
     * @param ttl
     */
    public void setTtl(Long ttl) {
        this.ttl = ttl;
    }

    /**
     * Get whether the result is updatable and therefore sensitive to data changes.
     * @return
     */
    public Boolean getUpdatable() {
        return updatable;
    }

    public void setUpdatable(Boolean updatable) {
        this.updatable = updatable;
    }

    public Scope getScope() {
        return this.scope;
    }

    public void setScope(Scope scope) {
        this.scope = scope;
    }

    /**
     * Whether the engine should read and cache the entire results.
     * @return
     */
    public Boolean getReadAll() {
        return readAll;
    }

    public void setReadAll(Boolean readAll) {
        this.readAll = readAll;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof CacheDirective)) {
            return false;
        }
        CacheDirective other = (CacheDirective)obj;
        return EquivalenceUtil.areEqual(this.prefersMemory, other.prefersMemory)
        && EquivalenceUtil.areEqual(this.readAll, other.readAll)
        && EquivalenceUtil.areEqual(this.ttl, other.ttl)
        && EquivalenceUtil.areEqual(this.updatable, other.updatable)
        && EquivalenceUtil.areEqual(this.scope, other.scope)
        && EquivalenceUtil.areEqual(this.invalidation, other.invalidation);
    }

    @Override
    public int hashCode() {
        return HashCodeUtil.hashCode(1, scope, ttl, updatable);
    }

    public Invalidation getInvalidation() {
        return invalidation;
    }

    public void setInvalidation(Invalidation invalidation) {
        this.invalidation = invalidation;
    }

}
