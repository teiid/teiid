/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import com.metamatrix.data.language.*;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;

/**
 */
public class SubqueryInCriteriaImpl extends BaseLanguageObject implements ISubqueryInCriteria {

    private IExpression leftExpr;
    private boolean isNegated = false;
    private IQueryCommand rightQuery;

    /**
     * 
     */
    public SubqueryInCriteriaImpl(IExpression leftExpr, boolean isNegated, IQueryCommand rightQuery) {
        this.leftExpr = leftExpr;
        this.isNegated = isNegated;
        this.rightQuery = rightQuery;
    }

    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#getLeftExpression()
     */
    public IExpression getLeftExpression() {
        return this.leftExpr;
    }

    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#isNegated()
     */
    public boolean isNegated() {
        return this.isNegated;
    }

    /* 
     * @see com.metamatrix.data.language.ISubqueryContainer#getQuery()
     */
    public IQueryCommand getQuery() {
        return this.rightQuery;
    }

    /* 
     * @see com.metamatrix.data.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#setLeftExpression(com.metamatrix.data.language.IExpression)
     */
    public void setLeftExpression(IExpression expression) {
        this.leftExpr = expression;        
    }

    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#setNegated(boolean)
     */
    public void setNegated(boolean negated) {
        this.isNegated = negated;
    }

    /* 
     * @see com.metamatrix.data.language.ISubqueryContainer#setQuery(com.metamatrix.data.language.IQuery)
     */
    public void setQuery(IQueryCommand query) {
        this.rightQuery = query;
    }

}
