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

package org.teiid.connector.language;

/**
 * Represents an expression in the SELECT clause.  Anything 
 * other than an aggregate symbol in the SELECT clause will
 * be referenced by an IExpressionSymbol.
 */
public interface IAggregate extends IExpression {

    public static final String COUNT = "COUNT"; //$NON-NLS-1$
    public static final String AVG = "AVG"; //$NON-NLS-1$
    public static final String SUM = "SUM"; //$NON-NLS-1$
    public static final String MIN = "MIN"; //$NON-NLS-1$
    public static final String MAX = "MAX";     //$NON-NLS-1$

    /** 
     * Get the name of the aggregate function.  This will be one of the constants defined
     * in this interface.
     * @see #COUNT
     * @see #AVG
     * @see #SUM
     * @see #MIN
     * @see #MAX
     */
    String getName();

    /**
     * Set the name of the aggregate function.  This will be one of the constants defined
     * in this interface.
     * @param name New aggregate function name
     * @see #COUNT
     * @see #AVG
     * @see #SUM
     * @see #MIN
     * @see #MAX
     */
    void setName(String name);

    /**
     * Determine whether this function was executed with DISTINCT.  Executing 
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.  
     * @return True if DISTINCT mode is used 
     */
    boolean isDistinct();

    /**
     * Set whether this function was executed with DISTINCT.  Executing 
     * with DISTINCT will remove all duplicate values in a group when evaluating
     * the aggregate function.  
     * @param isDistinct True if DISTINCT mode should be used 
     */
    void setDistinct(boolean isDistinct);

    /**
     * Get the expression within the aggregate function.  The expression will be 
     * null for the special case COUNT(*).  This is the only case where the 
     * expression will be null
     * @return The expression or null for COUNT(*)
     */
    IExpression getExpression();
    
    /**
     * Set the expression within the aggregate function.  The expression will be
     * null for the special case COUNT(*). 
     * @param expression The new expression
     */
    void setExpression(IExpression expression);
    
}
