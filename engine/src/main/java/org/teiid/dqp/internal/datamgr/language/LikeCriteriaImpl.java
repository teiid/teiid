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

import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.ILikeCriteria;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class LikeCriteriaImpl extends BaseLanguageObject implements ILikeCriteria {
        
    private IExpression leftExpression = null;
    private IExpression rightExpression = null;
    private Character escapeCharacter = null;
    private boolean isNegated = false;
    
    public LikeCriteriaImpl(IExpression left, IExpression right, Character escapeCharacter, boolean negated) {
        leftExpression = left;
        rightExpression = right;
        this.escapeCharacter = escapeCharacter;
        this.isNegated = negated;
        
    }

    /**
     * @see org.teiid.connector.language.ILikeCriteria#getLeftExpression()
     */
    public IExpression getLeftExpression() {
        return leftExpression;
    }

    /**
     * @see org.teiid.connector.language.ILikeCriteria#getRightExpression()
     */
    public IExpression getRightExpression() {
        return rightExpression;
    }

    /**
     * @see org.teiid.connector.language.ILikeCriteria#getEscapeCharacter()
     */
    public Character getEscapeCharacter() {
        return this.escapeCharacter;
    }

    /**
     * @see org.teiid.connector.language.ILikeCriteria#isNegated()
     */
    public boolean isNegated() {
        return this.isNegated;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.ILikeCriteria#setLeftExpression(com.metamatrix.data.language.IExpression)
     */
    public void setLeftExpression(IExpression expression) {
        this.leftExpression = expression;        
    }

    /* 
     * @see com.metamatrix.data.language.ILikeCriteria#setRightExpression(com.metamatrix.data.language.IExpression)
     */
    public void setRightExpression(IExpression expression) {
        this.rightExpression = expression;
    }

    /* 
     * @see com.metamatrix.data.language.ILikeCriteria#setEscapeCharacter(java.lang.Character)
     */
    public void setEscapeCharacter(Character character) {
        this.escapeCharacter = character;
    }

    /* 
     * @see com.metamatrix.data.language.ILikeCriteria#setNegated(boolean)
     */
    public void setNegated(boolean negated) {
        this.isNegated = negated;
    }

}
