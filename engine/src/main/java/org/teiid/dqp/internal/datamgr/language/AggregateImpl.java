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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.IAggregate;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class AggregateImpl extends BaseLanguageObject implements IAggregate {
    
    private IExpression expression = null;
    private String aggName;
    private boolean isDistinct;
    private Class type;
    
    public AggregateImpl(String aggName, boolean isDistinct, IExpression exp, Class type) {
        this.expression = exp;
        this.aggName = aggName;
        this.isDistinct = isDistinct;
        this.type = type;
    }

    /**
     * @see org.teiid.connector.language.IAggregate#getName()
     */
    public String getName() {
        return this.aggName;
    }

    /**
     * @see org.teiid.connector.language.IAggregate#isDistinct()
     */
    public boolean isDistinct() {
        return this.isDistinct;
    }

    /**
     * @see org.teiid.connector.language.IAggregate#getExpression()
     */
    public IExpression getExpression() {
        return this.expression;
    }
    
    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IAggregate#setName(java.lang.String)
     */
    public void setName(String name) {
        this.aggName = name;
    }

    /* 
     * @see com.metamatrix.data.language.IAggregate#setDistinct(boolean)
     */
    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /* 
     * @see com.metamatrix.data.language.IAggregate#setExpression(com.metamatrix.data.language.IExpression)
     */
    public void setExpression(IExpression expression) {
        this.expression = expression;
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
