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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;


/**
 * A criteria which is true is the expression's value is a member in a list 
 * of values.  This criteria can be represented as "<expression> IN (<expr>, ...)".  
 */
public class SetCriteria extends AbstractSetCriteria {

	/** The set of value expressions */
    private Collection values;
    private boolean allConstants;

    /**
     * Constructs a default instance of this class.
     */
    public SetCriteria() {}

    /**
     * Constructs an instance of this class with the membership expression and value expressions
     * @param expression The membership expression 
     * @param values   The set of value {@link org.teiid.query.sql.symbol.Expression}s
     */
    public SetCriteria( Expression expression, Collection values ) {
        set(expression,values);
    }

    /**
     * Returns the number of values in the set.
     * @return Number of values in set
     */
    public int getNumberOfValues() {
        return (this.values != null) ? this.values.size() : 0;
    }

    /**
     * Returns the set of values.  Returns an empty collection if there are
     * currently no values.
     * @return The collection of Expression values
     */
    public Collection getValues() {
        return this.values;
    }

    /**
     * Sets the values in the set.
     * @param values The set of value Expressions
     */
    public void setValues( Collection values ) {
        this.values = values;
    }
    
    /**
     * Sets the membership expression and the set of value expressions
     * @param expression The membership expression
     * @param values   The set of value expressions
     */
    public void set( Expression expression, Collection values ) {
        setExpression(expression);
        setValues(values);
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

		// The expHashCode method walks the set of values, combining
		// the hash code at every power of 2: 1,2,4,8,...  This is
		// much quicker than calculating hash codes for ALL values
		
		hc = HashCodeUtil.expHashCode(hc, getValues());
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
		
		if(! (obj instanceof SetCriteria)) {
    		return false;
		}

        SetCriteria sc = (SetCriteria)obj;

        if (isNegated() ^ sc.isNegated()) {
            return false;
        }
                
        return getValues().size() == sc.getValues().size() && 
        	getValues().containsAll(sc.getValues()) &&
            EquivalenceUtil.areEqual(getExpression(), sc.getExpression());
	}
	
	/**
	 * Deep copy of object
	 * @return Deep copy of object
	 */
	public Object clone() {
	    Expression copy = null;
	    if(getExpression() != null) { 
	        copy = (Expression) getExpression().clone();
	    }	
	    
	    Collection copyValues = null;
	    if (isAllConstants()) {
	    	copyValues = new LinkedHashSet(values);
	    } else {
	    	copyValues = LanguageObject.Util.deepClone(new ArrayList(values), Expression.class);
	    }
	    
        SetCriteria criteriaCopy = new SetCriteria(copy, copyValues);
        criteriaCopy.setNegated(isNegated());
        criteriaCopy.allConstants = allConstants;
        return criteriaCopy;
	}
	
	public boolean isAllConstants() {
		return allConstants;
	}
	
	public void setAllConstants(boolean allConstants) {
		this.allConstants = allConstants;
	}

}  // END CLASS
