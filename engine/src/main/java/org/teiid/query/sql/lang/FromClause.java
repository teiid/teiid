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

import java.util.Collection;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.lang.Option.MakeDep;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * A FromClause is an interface for subparts held in a FROM clause.  One
 * type of FromClause is {@link UnaryFromClause}, which is the more common
 * use and represents a single group.  Another, less common type of FromClause
 * is the {@link JoinPredicate} which represents a join between two FromClauses
 * and may contain criteria.
 */
public abstract class FromClause implements LanguageObject {

    public static final String PRESERVE = "PRESERVE"; //$NON-NLS-1$

    private boolean optional;
    private MakeDep makeDep;
    private boolean makeNotDep;
    private MakeDep makeInd;
    private boolean noUnnest;
    private boolean preserve;

    public boolean isOptional() {
        return optional;
    }

    public void setOptional(boolean optional) {
        this.optional = optional;
    }

    public MakeDep getMakeInd() {
        return makeInd;
    }

    public void setMakeInd(MakeDep makeInd) {
        this.makeInd = makeInd;
    }

    public abstract void acceptVisitor(LanguageVisitor visitor);
    public abstract void collectGroups(Collection<GroupSymbol> groups);
    protected abstract FromClause cloneDirect();

    public FromClause clone() {
        FromClause clone = cloneDirect();
        clone.makeDep = makeDep;
        clone.makeInd = makeInd;
        clone.makeNotDep = makeNotDep;
        clone.optional = optional;
        clone.noUnnest = noUnnest;
        clone.preserve = preserve;
        return clone;
    }

    public void setNoUnnest(boolean noUnnest) {
        this.noUnnest = noUnnest;
    }

    public boolean isNoUnnest() {
        return noUnnest;
    }

    public boolean isMakeDep() {
        return this.makeDep != null;
    }

    public MakeDep getMakeDep() {
        return makeDep;
    }

    public void setMakeDep(boolean makeDep) {
        if (makeDep) {
            if (this.makeDep == null) {
                this.makeDep = new MakeDep();
            }
        } else {
            this.makeDep = null;
        }
    }

    public boolean isMakeNotDep() {
        return this.makeNotDep;
    }

    public void setMakeNotDep(boolean makeNotDep) {
        this.makeNotDep = makeNotDep;
    }

    public void setMakeDep(MakeDep makedep) {
        this.makeDep = makedep;
    }

    public boolean isPreserve() {
        return preserve;
    }

    public void setPreserve(boolean preserve) {
        this.preserve = preserve;
    }

    public boolean hasHint() {
        return optional || makeDep != null || makeNotDep || makeInd != null || noUnnest || preserve;
    }

    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(! (obj instanceof FromClause)) {
            return false;
        }

        FromClause other = (FromClause)obj;

        return other.isOptional() == this.isOptional()
               && EquivalenceUtil.areEqual(this.makeDep, other.makeDep)
               && other.isMakeNotDep() == this.isMakeNotDep()
               && EquivalenceUtil.areEqual(this.makeInd, other.makeInd)
               && other.isNoUnnest() == this.isNoUnnest()
               && other.isNoUnnest() == this.isNoUnnest();
    }

    @Override
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
}
