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
 * Represents a comparison between two expressions connected with
 * one of the following operators: =, <>, <, <=, >, >=.
 */
public interface ICompareCriteria extends IPredicateCriteria {

	public enum Operator {
		EQ,
		NE,
		LT,
		LE,
		GT,
		GE
	}
	
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
     * @see Operator#EQ
     * @see Operator#NE
     * @see Operator#LT
     * @see Operator#LE
     * @see Operator#GT
     * @see Operator#GE
     */
    Operator getOperator();
    
    /**
     * Set operator from set defined in this interface.
     * @param operator Operator constant
     * @see Operator#EQ
     * @see Operator#NE
     * @see Operator#LT
     * @see Operator#LE
     * @see Operator#GT
     * @see Operator#GE
     */
    void setOperator(Operator operator);
}
