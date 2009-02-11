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

package com.metamatrix.connector.language;

/**
 * Represents a comparison between two expressions connected with
 * one of the following operators: =, <>, <, <=, >, >=.
 */
public interface ICompareCriteria extends IPredicateCriteria {

    public static final int EQ = 0;
    public static final int NE = 1;
    public static final int LT = 2;
    public static final int LE = 3;
    public static final int GT = 4;
    public static final int GE = 5;

    /**
     * Get left expression.
     * @return Left expression
     */
    IExpression getLeftExpression();

    /**
     * Set left expression of criteria
     * @return Left expression
     */
    void setLeftExpression(IExpression expression);
    
    /**
     * Get right expression.
     * @return Right expression
     */
    IExpression getRightExpression();

    /**
     * Set left expression of criteria
     * @return Right expression
     */
    void setRightExpression(IExpression expression);
    
    /**
     * Get operator from set defined in this interface.
     * @return Operator constant
     * @see #EQ
     * @see #NE
     * @see #LT
     * @see #LE
     * @see #GT
     * @see #GE
     */
    int getOperator();
    
    /**
     * Set operator from set defined in this interface.
     * @param operator Operator constant
     * @see #EQ
     * @see #NE
     * @see #LT
     * @see #LE
     * @see #GT
     * @see #GE
     */
    void setOperator(int operator);
}
