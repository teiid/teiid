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

package org.teiid.query.sql.symbol;

import java.util.Collections;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.visitor.SQLStringVisitor;



public abstract class AbstractCaseExpression implements Expression {
    
    /** The type that this case expression will resolve to. */
    private Class type = null;
    /** Ordered List containing Expression objects. */
    private List then = null;
    /** The (optional) expression in the ELSE part of the expression */
    private Expression elseExpression = null;
    
    
    protected AbstractCaseExpression() {
    }

    /**
     * Gets the number of WHEN and THEN parts this case expression contains.
     * This number is always &gt;= 1.
     * @return
     */
    public abstract int getWhenCount();
    
    /**
     * Gets the expression of the THEN part at the given index.
     * @param index
     * @return
     */
    public Expression getThenExpression(int index) {
        return (Expression)then.get(index);
    }
    
    /**
     * Gets the List of THEN expressions in this CASE expression. Never null.
     * @return
     */
    public List getThen() {
        return then;
    }
    
    /**
     * Sets the List of THEN expressions in this CASE expression
     * @param then
     */
    protected void setThen(List then) {
        if (this .then != then) {
            this.then = Collections.unmodifiableList(then);
        }
    }
    
    /**
     * Gets the expression in the ELSE part of this expression. May be null as
     * the ELSE is optional.
     * @return
     */
    public Expression getElseExpression() {
        return elseExpression;
    }
    
    /**
     * Sets the expression in the ELSE part of this expression. Can be null.
     * @param elseExpression
     */
    public void setElseExpression(Expression elseExpression) {
        this.elseExpression = elseExpression;
    }
    
    /**
     * @see org.teiid.query.sql.symbol.Expression#getType()
     */
    public Class getType() {
        return type;
    }
    
    /**
     * Sets the type to which this expression has resolved.
     * @param type
     */
    public void setType(Class type) {
        this.type = type;
    }
    
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof AbstractCaseExpression)) return false;
        AbstractCaseExpression other = (AbstractCaseExpression)obj;
        return (getThen().equals(other.getThen()) &&
                EquivalenceUtil.areEqual(getElseExpression(), other.getElseExpression()) &&
                EquivalenceUtil.areEqual(getType(), other.getType())); 
    }
    
    public int hashCode() {
        int hash = 0;
        if(then != null) {
            for(int i=0; i<then.size(); i++) {
                hash = HashCodeUtil.hashCode(hash, then.get(i));
            }
        }
        if(elseExpression != null) {
            hash = HashCodeUtil.hashCode(hash, elseExpression);
        }
        return hash;
    }
    
    public abstract Object clone();

    /**
     * Return a String representation of this object using SQLStringVisitor.
     * @return String representation using SQLStringVisitor
     */
    public String toString() {
        return SQLStringVisitor.getSQLString(this);
    }
    
    public abstract void setWhen( List whens, List thens);

}
