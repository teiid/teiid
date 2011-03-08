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

package org.teiid.query.sql.lang;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * <p>A criteria which represents a simple operator relationship between two expressions.
 * There are 6 operator types.  Each side of the comparison may be an expression, which 
 * could be an element, a constant, or a function.  </p>
 *
 * <p>Some examples are:</p>
 * <UL>
 * <LI>ticker = 'IBM'</LI>
 * <li>price &gt;= 50</LI>
 * <LI>revenue &lt; 0</LI>
 * <LI>5 &lt;= length(companyName)</LI>
 * </UL>
 */
public class CompareCriteria extends AbstractCompareCriteria {

	/** The right-hand expression. */
	private Expression rightExpression;
	private Boolean isOptional = Boolean.FALSE;
    
    /**
     * Constructs a default instance of this class.
     */
    public CompareCriteria() {
        super();
    }
    
    /**
     * Constructs an instance of this class for a specific "operand operator
     * operand" clause.
     * 
     * @param identifier The variable being compared
     * @param value The value the variable is being compared to (literal or variable)
     * @param operator The operator representing how the variable and value are to
     *                 be compared
     */
    public CompareCriteria( Expression leftExpression, int operator, Expression rightExpression ) {
        super();
        set(leftExpression, operator, rightExpression);
    }

	/**
	 * Set right expression.
	 * @param expression Right expression
	 */
	public void setRightExpression(Expression expression) { 
		this.rightExpression = expression;
	}
	
	/**
	 * Get right expression.
	 * @return right expression
	 */
	public Expression getRightExpression() {
		return this.rightExpression;
	}
	
    /**
     * Sets the operands and operator.  The clause is of the form: <variable> <operator> <value>.
     *
     * @param leftExpression The left expression
     * @param operator The operator representing how the expressions are compared
     * @param rightExpression The right expression
     */
    public void set( Expression leftExpression, int operator, Expression rightExpression ) {
        setLeftExpression(leftExpression);
        setOperator(operator);
        setRightExpression(rightExpression);
    }
    
    /**
     * Set during planning to indicate that this criteria is no longer needed
     * to correctly process a join
     * @param isOptional
     */
    public void setOptional(Boolean isOptional) {
		this.isOptional = isOptional;
	}
    
    /**
     * Returns true if the compare criteria is used as join criteria, but not needed
     * during processing.
     * @return
     */
    public boolean isOptional() {
		return isOptional == null || isOptional;
	}
    
    public Boolean getIsOptional() {
		return isOptional;
	}

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
	 * Get hash code.  WARNING: The hash code is based on data in the criteria.
	 * If data values are changed, the hash code will change - don't hash this
	 * object and change values.
	 * @return Hash code
	 */
	public int hashCode() {
		int hc = 0;
		hc = HashCodeUtil.hashCode(hc, getOperator());
		hc = HashCodeUtil.hashCode(hc, getLeftExpression());
		hc = HashCodeUtil.hashCode(hc, getRightExpression());
		return hc;
	}

    /**
     * Override equals() method.
     * @param obj Other object
     * @return true if objects are equivalent
     */
    public boolean equals(Object obj) {
    	// Use super.equals() to check obvious stuff and variable
    	if(obj == this) {
			return true;
		}
		
		if(! (obj instanceof CompareCriteria)) {
    		return false;
		}

        CompareCriteria cc = (CompareCriteria)obj;
        return getOperator() == cc.getOperator() &&
               EquivalenceUtil.areEqual(getLeftExpression(), cc.getLeftExpression()) &&
               EquivalenceUtil.areEqual(getRightExpression(), cc.getRightExpression());
	}
	
	/**
	 * Deep copy of object
	 * @return Deep copy of object
	 */
	public Object clone() {
	    Expression leftCopy = null;
	    if(getLeftExpression() != null) { 
	        leftCopy = (Expression) getLeftExpression().clone();
	    }	
	    Expression rightCopy = null;
	    if(getRightExpression() != null) { 
	        rightCopy = (Expression) getRightExpression().clone();
	    }	
	    
		CompareCriteria result = new CompareCriteria(leftCopy, getOperator(), rightCopy);
		result.isOptional = isOptional;
		return result;
	}
	
}  // END CLASS
