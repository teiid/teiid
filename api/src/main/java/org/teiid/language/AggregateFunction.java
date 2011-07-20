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

import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents an aggregate function.
 */
public class AggregateFunction extends BaseLanguageObject implements Expression {
    
    public static final String COUNT = "COUNT"; //$NON-NLS-1$
    public static final String AVG = "AVG"; //$NON-NLS-1$
    public static final String SUM = "SUM"; //$NON-NLS-1$
    public static final String MIN = "MIN"; //$NON-NLS-1$
    public static final String MAX = "MAX";     //$NON-NLS-1$
    public static final String STDDEV_POP = "STDDEV_POP"; //$NON-NLS-1$
	public static final String STDDEV_SAMP = "STDDEV_SAMP"; //$NON-NLS-1$
	public static final String VAR_SAMP = "VAR_SAMP"; //$NON-NLS-1$
	public static final String VAR_POP = "VAR_POP"; //$NON-NLS-1$
	
    private Expression expression;
    private String aggName;
    private boolean isDistinct;
    private Class<?> type;
    private Expression condition;
    
    public AggregateFunction(String aggName, boolean isDistinct, Expression exp, Class<?> type) {
        this.expression = exp;
        this.aggName = aggName;
        this.isDistinct = isDistinct;
        this.type = type;
    }

    /** 
     * Get the name of the aggregate function.  This will be one of the constants defined
     * in this class.
     */
    public String getName() {
        return this.aggName;
    }

    /**
     * Determine whether this function was executed with DISTINCT.  Executing 
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.  
     * @return True if DISTINCT mode is used 
     */
    public boolean isDistinct() {
        return this.isDistinct;
    }

    /**
     * Get the expression within the aggregate function.  The expression will be 
     * null for the special case COUNT(*).  This is the only case where the 
     * expression will be null
     * @return The expression or null for COUNT(*)
     */
    public Expression getExpression() {
        return this.expression;
    }
    
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Set the name of the aggregate function.  This will be one of the constants defined
     * in this class.
     * @param name New aggregate function name
     */
    public void setName(String name) {
        this.aggName = name;
    }

    /**
     * Set whether this function was executed with DISTINCT.  Executing 
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.  
     * @param isDistinct True if DISTINCT mode should be used 
     */
    public void setDistinct(boolean isDistinct) {
        this.isDistinct = isDistinct;
    }

    /**
     * Set the expression within the aggregate function.  The expression will be
     * null for the special case COUNT(*). 
     * @param expression The new expression
     */
    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public Class<?> getType() {
        return this.type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }
    
    /**
     * 
     * @return the filter clause condition
     */
    public Expression getCondition() {
		return condition;
	}
    
    public void setCondition(Expression condition) {
		this.condition = condition;
	}

}
