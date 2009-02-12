/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package com.metamatrix.query.sql.lang;

import java.util.Collection;

import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;

/**
 * A FromClause is an interface for subparts held in a FROM clause.  One 
 * type of FromClause is {@link UnaryFromClause}, which is the more common 
 * use and represents a single group.  Another, less common type of FromClause
 * is the {@link JoinPredicate} which represents a join between two FromClauses
 * and may contain criteria.
 */
public abstract class FromClause implements LanguageObject {
    private boolean optional;
    private boolean makeDep;
    private boolean makeNotDep;

    public boolean isOptional() {
        return optional;
    }
    
    public void setOptional(boolean optional) {
        this.optional = optional;
    }
    
    public abstract void acceptVisitor(LanguageVisitor visitor);
    public abstract void collectGroups(Collection groups);
    public abstract Object clone();

    public boolean isMakeDep() {
        return this.makeDep;
    }

    public void setMakeDep(boolean makeDep) {
        this.makeDep = makeDep;
    }

    public boolean isMakeNotDep() {
        return this.makeNotDep;
    }

    public void setMakeNotDep(boolean makeNotDep) {
        this.makeNotDep = makeNotDep;
    }
    
    public boolean hasHint() {
        return optional || makeDep || makeNotDep;
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
               && other.isMakeDep() == this.isMakeDep()
               && other.isMakeNotDep() == this.isMakeNotDep();
    }
}
