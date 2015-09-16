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
import org.teiid.query.sql.lang.PredicateCriteria.Negatable;
import org.teiid.query.sql.symbol.GroupSymbol;


/**
 * Represents criteria such as:  "<expression> IS DISTINCT FROM <expression>".
 * However due to a lack of direct support for new/old groups as row values,
 * we reference group symbols here instead.
 */
public class IsDistinctCriteria extends PredicateCriteria implements Negatable {

	private GroupSymbol leftRowValue;
	private GroupSymbol rightRowValue;
    /** Negation flag. Indicates whether the criteria expression contains a NOT. */
    private boolean negated;
	
    /**
     * Constructs a default instance of this class.
     */
    public IsDistinctCriteria() {}

    public void setLeftRowValue(GroupSymbol leftRowValue) {
		this.leftRowValue = leftRowValue;
	}
    
    public void setRightRowValue(GroupSymbol rightRowValue) {
		this.rightRowValue = rightRowValue;
	}
    
    public GroupSymbol getLeftRowValue() {
		return leftRowValue;
	}
    
    public GroupSymbol getRightRowValue() {
		return rightRowValue;
	}
	
    /**
     * Returns whether this criteria is negated.
     * @return flag indicating whether this criteria contains a NOT
     */
    public boolean isNegated() {
        return negated;
    }
    
    /**
     * Sets the negation flag for this criteria.
     * @param negationFlag true if this criteria contains a NOT; false otherwise
     */
    public void setNegated(boolean negationFlag) {
        negated = negationFlag;
    }
    
    @Override
    public void negate() {
    	this.negated = !this.negated;
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
	 * Get hash code.  WARNING: The hash code is based on data in the criteria.
	 * If data values are changed, the hash code will change - don't hash this
	 * object and change values.
	 * @return Hash code for object
	 */
	public int hashCode() {
		return HashCodeUtil.hashCode(negated?0:1, leftRowValue, rightRowValue);
	}
	
    /**
     * Compares this criteria to another object for equality
     * @param obj Other object
     * @return True if objects are equal
     */
    public boolean equals(Object obj) {
		if(this == obj) { 
			return true;
		}
		
		if(! (obj instanceof IsDistinctCriteria)) {
			return false;
		} 
		
		IsDistinctCriteria other = (IsDistinctCriteria) obj;
        if (isNegated() ^ other.isNegated()) {
            return false;
        }
        return EquivalenceUtil.areEqual(leftRowValue, other.leftRowValue)
        		&& EquivalenceUtil.areEqual(rightRowValue, other.rightRowValue);			
	}
	
	/**
	 * Deep copy of object
	 * @return Deep copy of object
	 */
	public Object clone() {
        IsDistinctCriteria criteriaCopy = new IsDistinctCriteria();
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.setLeftRowValue(this.getLeftRowValue().clone());
        criteriaCopy.setRightRowValue(this.getRightRowValue().clone());
		return criteriaCopy;
	}
	
}