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

import com.metamatrix.connector.language.ICaseExpression;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;

public class CaseExpressionImpl extends BaseLanguageObject implements ICaseExpression {
    
    private IExpression expression = null;
    private List whenExpressions = null;
    private List thenExpressions = null;
    private IExpression elseExpression = null;
    private Class type = null;

    public CaseExpressionImpl(IExpression expression,
                              List whens, List thens, IExpression elseExpression,
                              Class type) {

        this.expression = expression;
        this.whenExpressions = whens;
        this.thenExpressions = thens;
        this.elseExpression = elseExpression;
        this.type = type;
    }
    /**
     * @see com.metamatrix.connector.language.ICaseExpression#getElseExpression()
     */
    public IExpression getElseExpression() {
        return elseExpression;
    }

    /**
     * @see com.metamatrix.connector.language.ICaseExpression#getExpression()
     */
    public IExpression getExpression() {
        return expression;
    }

    /**
     * @see com.metamatrix.connector.language.ICaseExpression#getThenExpression(int)
     */
    public IExpression getThenExpression(int index) {
        return (IExpression)thenExpressions.get(index);
    }

    /**
     * @see com.metamatrix.connector.language.ICaseExpression#getWhenCount()
     */
    public int getWhenCount() {
        return whenExpressions.size();
    }

    /**
     * @see com.metamatrix.connector.language.ICaseExpression#getWhenExpression(int)
     */
    public IExpression getWhenExpression(int index) {
        return (IExpression)whenExpressions.get(index);
    }

    /**
     * @see com.metamatrix.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    /* 
     * @see com.metamatrix.data.language.ICaseExpression#setExpression(com.metamatrix.data.language.IExpression)
     */
    public void setExpression(IExpression expression) {
        this.expression = expression;
    }
    /* 
     * @see com.metamatrix.data.language.ICaseExpression#setWhenExpression(int, com.metamatrix.data.language.IExpression)
     */
    public void setWhenExpression(int index, IExpression expression) {
        this.whenExpressions.set(index, expression);
    }

    /* 
     * @see com.metamatrix.data.language.ICaseExpression#setThenExpression(int, com.metamatrix.data.language.IExpression)
     */
    public void setThenExpression(int index, IExpression expression) {
        this.thenExpressions.set(index, expression);
    }
    
    /* 
     * @see com.metamatrix.data.language.ICaseExpression#setElseExpression(com.metamatrix.data.language.IExpression)
     */
    public void setElseExpression(IExpression expression) {
        this.elseExpression = expression;
    }
    
    /* 
     * @see com.metamatrix.data.language.IExpression#getType()
     */
    public Class getType() {
        return this.type;
    }
    /* 
     * @see com.metamatrix.data.language.IExpression#setType(java.lang.Class)
     */
    public void setType(Class type) {
        this.type = type;
    }

}
