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

import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * <p>The common functionality of a {@link CompareCriteria} and a
 * {@link SubqueryCompareCriteria}.  The comparison operators are defined
 * here.</p>
 */
public abstract class AbstractCompareCriteria extends PredicateCriteria {

    /** Constant indicating the two operands are equal. */
    public static final int EQ = 1;

    /** Constant indicating the two operands are not equal. */
    public static final int NE = 2;

    /** Constant indicating the first operand is less than the second. */
    public static final int LT = 3;

    /** Constant indicating the first operand is greater than the second. */
    public static final int GT = 4;

    /** Constant indicating the first operand is less than or equal to the second. */
    public static final int LE = 5;

    /** Constant indicating the first operand is greater than or equal to the second. */
    public static final int GE = 6;

	/** The left-hand expression. */
	private Expression leftExpression;

    /**
     * The operator used in the clause.
     * @see #EQ
     * @see #NE
     * @see #LT
     * @see #GT
     * @see #LE
     * @see #GE
     */
    private int operator = EQ;

//    /**
//     * Constructs a default instance of this class.
//     */
//    public AbstractCompareCriteria() {
//        super();
//    }

//    /**
//     * Constructs an instance of this class for a specific "operand operator
//     * operand" clause.
//     *
//     * @param identifier The variable being compared
//     * @param value The value the variable is being compared to (literal or variable)
//     * @param operator The operator representing how the variable and value are to
//     *                 be compared
//     */
//    public AbstractCompareCriteria( Expression leftExpression, int operator, Expression rightExpression ) {
//        super();
//        set(leftExpression, operator, rightExpression);
//    }

    /**
     * Returns the operator.
     * @return The operator
     */
    public int getOperator() {
        return this.operator;
    }

    /**
     * Sets the operator.
     * @param operator
     */
    public void setOperator( int operator ) {
        if (operator < EQ || operator > GE) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0001, operator));
        }
        this.operator = operator;
    }

    /**
    * Gets the operator constant given the string version
    * @param op Operator, string form
    * @return Operator, constant form
    */
    public static int getOperator(String op){
        if(op.equals("=" )) return EQ; //$NON-NLS-1$
        else if(op.equals("<>")) return NE; //$NON-NLS-1$
        else if(op.equals("<>")) return NE; //$NON-NLS-1$
        else if(op.equals("<" )) return LT; //$NON-NLS-1$
        else if(op.equals(">" )) return GT; //$NON-NLS-1$
        else if(op.equals("<=")) return LE; //$NON-NLS-1$
        else if(op.equals(">=")) return GE; //$NON-NLS-1$
        else return -1;
    }

	/**
	 * Set left expression.
	 * @param expression Left expression
	 */
	public void setLeftExpression(Expression expression) {
		this.leftExpression = expression;
	}

	/**
	 * Get left expression.
	 * @return Left expression
	 */
	public Expression getLeftExpression() {
		return this.leftExpression;
	}
    
    public abstract Expression getRightExpression();

    /**
     * Returns the operator as a string.
     * @return String version of operator
     */
    public String getOperatorAsString() {
        switch ( this.operator ) {
            case EQ: return "="; //$NON-NLS-1$
            case NE: return "<>"; //$NON-NLS-1$
            case LT: return "<"; //$NON-NLS-1$
            case GT: return ">"; //$NON-NLS-1$
            case LE: return "<="; //$NON-NLS-1$
            case GE: return ">="; //$NON-NLS-1$
            default: return "??"; //$NON-NLS-1$
        }
    }

    
}  // END CLASS
