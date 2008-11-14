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

import java.util.List;

import com.metamatrix.data.language.IExpression;
import com.metamatrix.data.language.IInCriteria;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;

public class InCriteriaImpl extends BaseLanguageObject implements IInCriteria {

    private IExpression leftExpression = null;
    private List rightExpressions = null;
    private boolean negated = false;
    
    public InCriteriaImpl(IExpression left, List right, boolean negated) {
        leftExpression = left;
        rightExpressions = right;
        this.negated = negated;
    }
    /**
     * @see com.metamatrix.data.language.IInCriteria#getLeftExpression()
     */
    public IExpression getLeftExpression() {
        return leftExpression;
    }

    /**
     * @see com.metamatrix.data.language.IInCriteria#getRightExpressions()
     */
    public List getRightExpressions() {
        return rightExpressions;
    }

    /**
     * @see com.metamatrix.data.language.IInCriteria#isNegated()
     */
    public boolean isNegated() {
        return this.negated;
    }

    /**
     * @see com.metamatrix.data.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    /* 
     * @see com.metamatrix.data.language.IInCriteria#setRightExpressions(java.util.List)
     */
    public void setRightExpressions(List expressions) {
        this.rightExpressions = expressions;
    }
    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#setLeftExpression(com.metamatrix.data.language.IExpression)
     */
    public void setLeftExpression(IExpression expression) {
        this.leftExpression = expression;
    }
    /* 
     * @see com.metamatrix.data.language.IBaseInCriteria#setNegated(boolean)
     */
    public void setNegated(boolean negated) {
        this.negated = negated;
    }

}
