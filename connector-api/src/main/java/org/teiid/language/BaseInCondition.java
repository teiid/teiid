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

package org.teiid.language;

public abstract class BaseInCondition extends Condition implements Predicate {
	
    private Expression leftExpression;
    private boolean negated;
    
    public BaseInCondition(Expression leftExpression, boolean negated) {
    	this.leftExpression = leftExpression;
    	this.negated = negated;
	}

    /**
     * Get left expression of IN criteria
     * @return Left expression
     */
    public Expression getLeftExpression() {
		return leftExpression;
	}

    /**
     * Set left expression of IN criteria
     */
    public void setLeftExpression(Expression leftExpression) {
		this.leftExpression = leftExpression;
	}

    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
		return negated;
	}
    
    /**
     * Sets whether this criteria is negated.
     * @param negated Flag indicating whether this criteria contains a NOT
     */
    public void setNegated(boolean negated) {
		this.negated = negated;
	}
}
