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
import org.teiid.query.sql.symbol.ContextReference;
import org.teiid.query.sql.symbol.Expression;



/** 
 * The DependentSetCriteria is missing the value set until it is filled during
 * processing.  This allows a criteria to contain a dynamic set of values provided
 * by a separate processing node.
 * @since 5.0.1
 */
public class DependentSetCriteria extends AbstractSetCriteria implements ContextReference {
	
    /**
     * Specifies the expression whose values we want to return in the iterator
     */
    private Expression valueExpression;
    private String id;
    
    /** 
     * Construct with the left expression 
     */
    public DependentSetCriteria(Expression expr, String id) {
        setExpression(expr);
        this.id = id;
    }    
        
    @Override
    public String getContextSymbol() {
    	return id;
    }

    /** 
     * Get the independent value expression
     * @return Returns the valueExpression.
     */
    public Expression getValueExpression() {
        return this.valueExpression;
    }

    
    /**
     * Set the independent value expression 
     * @param valueExpression The valueExpression to set.
     */
    public void setValueExpression(Expression valueExpression) {
        this.valueExpression = valueExpression;
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
        hc = HashCodeUtil.hashCode(hc, getExpression());
        return hc;
    }

    /**
     * Override equals() method.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
        // Use super.equals() to check obvious stuff and variable
        if(obj == this) {
            return true;
        }

        if(!(obj instanceof DependentSetCriteria)) {
            return false;
        }

        DependentSetCriteria sc = (DependentSetCriteria)obj;
        if (isNegated() != sc.isNegated()) {
            return false;
        }
        
        return EquivalenceUtil.areEqual(getExpression(), sc.getExpression()) && 
                EquivalenceUtil.areEqual(getValueExpression(), sc.getValueExpression());
    }

    /**
     * Deep copy of object.  The values iterator source of this object
     * will not be cloned - it will be passed over as is and shared with 
     * the original object, just like Reference.
     * @return Deep copy of object
     */
    public Object clone() {
        Expression copy = null;
        if(getExpression() != null) {
            copy = (Expression) getExpression().clone();
        }

        DependentSetCriteria criteriaCopy = new DependentSetCriteria(copy, id);
        if (this.valueExpression != null) {
        	criteriaCopy.setValueExpression((Expression) getValueExpression().clone());
        }
        criteriaCopy.id = this.id;
        return criteriaCopy;
    }
    
    @Override
    public void setNegated(boolean negationFlag) {
    	if (!negationFlag) {
    		throw new UnsupportedOperationException();
    	}
    }
    
}
