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

package com.metamatrix.data.language;

/**
 * Represents a non-searched CASE expression:
 * <br/> CASE expression WHEN expression THEN expression ... END
 */
public interface ICaseExpression extends IExpression {
    
    /**
     * Gets the expression whose evaluation is tested against the WHEN expressions
     * @return the expression
     */
    IExpression getExpression();

    /**
     * Sets the expression whose evaluation is tested against the WHEN expressions
     * @param expression The expression
     */
    void setExpression(IExpression expression);
    
    /**
     * Gets the number of WHEN and THEN clauses in the CASE expression
     * @return the number of WHEN ... THEN ... parts
     */
    int getWhenCount();
    
    /**
     * Gets the WHEN expression at the specified index
     * @param index the 0-based index
     * @return  the WHEN expression at the index
     */
    IExpression getWhenExpression(int index);

    /**
     * Sets the WHEN expression at the specified index
     * @param index the 0-based index
     * @param expression The new WHEN expression at the index
     */
    void setWhenExpression(int index, IExpression expression);
    
    /**
     * Gets the THEN expression at the specified index
     * @param index the 0-based index
     * @return  the THEN expression at the index
     */
    IExpression getThenExpression(int index);

    /**
     * Sets the THEN expression at the specified index
     * @param index the 0-based index
     * @param expression The new THEN expression at the index
     */
    void setThenExpression(int index, IExpression expression);
    
    /**
     * Gets the ELSE expression, if defined. Can be null.
     * @return the ELSE expression.
     */
    IExpression getElseExpression();
    
    /**
     * Sets the ELSE expression 
     * @param expression The new ELSE expression 
     */
    void setElseExpression(IExpression expression);
    
}
