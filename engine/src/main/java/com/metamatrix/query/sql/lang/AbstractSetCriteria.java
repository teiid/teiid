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

import com.metamatrix.query.sql.symbol.Expression;

/**
 * This is an abstract class to define some common functionality in the two varieties of 
 * IN criteria:  {@link SetCriteria} (where values are specified) and {@link SubquerySetCriteria}
 * (where a subquery is defined and will supply the values for the IN set).  
 */
public abstract class AbstractSetCriteria extends PredicateCriteria {

    /** The left expression */
    private Expression expression;
    
    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated = false;

    /**
     * Constructor for AbstractSetCriteria.
     */
    protected AbstractSetCriteria() {
        super();
    }

    /**
     * Gets the membership expression to be compared.
     * @return The membership expression
     */
    public Expression getExpression() {
        return this.expression;
    }
    
    /**
     * Sets the membership expression
     * @param expression The membership expression
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }
    
    /**
     * Sets the negation flag for this criteria.
     * @param negationFlag true if this criteria contains a NOT; false otherwise
     */
    public void setNegated(boolean negationFlag) {
        negated = negationFlag;
    }

    /**
     * Deep copy of object
     * @return Deep copy of object
     */
    public abstract Object clone();
}
