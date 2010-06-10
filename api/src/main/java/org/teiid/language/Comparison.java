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

import org.teiid.language.SQLConstants.Tokens;
import org.teiid.language.visitor.LanguageObjectVisitor;

/**
 * Represents a comparison between two expressions connected with
 * one of the following operators: =, <>, <, <=, >, >=.
 */
public class Comparison extends Condition implements Predicate {

	public enum Operator {
		EQ(Tokens.EQ),
		NE(Tokens.NE),
		LT(Tokens.LT),
		LE(Tokens.LE),
		GT(Tokens.GT),
		GE(Tokens.GE);
		
		private String toString;
		Operator(String toString) {
			this.toString = toString;
		}
		@Override
		public String toString() {
			return toString;
		}
	}
	
    private Expression leftExpression;
    private Expression rightExpression;
    private Operator operator;
    
    public Comparison(Expression left, Expression right, Operator operator) {
        leftExpression = left;
        rightExpression = right;
        this.operator = operator;
    }
    
    /**
     * Get left expression.
     * @return Left expression
     */
    public Expression getLeftExpression() {
        return leftExpression;
    }

    /**
     * Get right expression.
     * @return Right expression
     */
    public Expression getRightExpression() {
        return rightExpression;
    }

    /**
     * Get the operator
     * @return Operator constant
     * @see Operator
     */
    public Operator getOperator() {
        return this.operator;
    }

    /**
     * Set left expression of criteria
     */
    public void setLeftExpression(Expression expression) {
        this.leftExpression = expression;
    }
    
    /**
     * Set right expression of criteria
     */
    public void setRightExpression(Expression expression) {
        this.rightExpression = expression;
    }
    
    /**
     * Set the operator
     * @see Operator
     */
    public void setOperator(Operator operator) {
        this.operator = operator;
    }
    
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    
}
