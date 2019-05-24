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

package org.teiid.query.sql.lang;

import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.sql.visitor.SQLStringVisitor;
import org.teiid.translator.CacheDirective;

public class CacheHint extends CacheDirective {

    private static final long serialVersionUID = -4119606289701982511L;

    public static final String PREF_MEM = "pref_mem"; //$NON-NLS-1$
    public static final String TTL = "ttl:"; //$NON-NLS-1$
    public static final String UPDATABLE = "updatable"; //$NON-NLS-1$
    public static final String CACHE = "cache"; //$NON-NLS-1$
    public static final String SCOPE = "scope:"; //$NON-NLS-1$

    public static final String MIN = "min:"; //$NON-NLS-1$

    private Long minRows;

    public CacheHint() {
    }

    public CacheHint(Boolean prefersMemory, Long ttl) {
        super(prefersMemory, ttl);
    }

    public boolean isPrefersMemory() {
        if (getPrefersMemory() != null) {
            return getPrefersMemory();
        }
        return false;
    }

    @Override
    public String toString() {
        SQLStringVisitor ssv = new SQLStringVisitor();
        ssv.addCacheHint(this);
        return ssv.getSQLString();
    }

    public Determinism getDeterminism() {
        if (this.getScope() == null) {
            return null;
        }
        switch (getScope()) {
        case SESSION:
            return Determinism.SESSION_DETERMINISTIC;
        case VDB:
            return Determinism.VDB_DETERMINISTIC;
        }
        return Determinism.USER_DETERMINISTIC;
    }

    public void setScope(String scope) {
        if (scope == null) {
            setScope((Scope)null);
        } else {
            setScope(Scope.valueOf(scope.toUpperCase()));
        }
    }

    public boolean isUpdatable(boolean b) {
        if (getUpdatable() != null) {
            return getUpdatable();
        }
        return b;
    }

    public CacheHint clone() {
        CacheHint copy = new CacheHint();
        copy.setInvalidation(this.getInvalidation());
        copy.setPrefersMemory(this.getPrefersMemory());
        copy.setReadAll(this.getReadAll());
        copy.setScope(this.getScope());
        copy.setTtl(this.getTtl());
        copy.setUpdatable(this.getUpdatable());
        copy.setMinRows(this.getMinRows());
        return copy;
    }

    public void setMinRows(Long minRows) {
        this.minRows = minRows;
    }

    public Long getMinRows() {
        return minRows;
    }

}
