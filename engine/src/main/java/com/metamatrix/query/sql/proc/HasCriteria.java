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

package com.metamatrix.query.sql.proc;

import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.*;
import com.metamatrix.query.sql.lang.PredicateCriteria;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p>This object represents the criteria used in the stored procedure language
 * to determine if a type of criteria is specified on a user's query.</p>
 */
public class HasCriteria extends PredicateCriteria {

	// the selector object used to determine if a type of criteria is specified 
	// on the user's query
	private CriteriaSelector criteriaSelector;
	
	/**
	 * Constructor for HasCriteria.
	 */
	public HasCriteria() {
	}
	
	/**
	 * Constructor for HasCriteria.
	 * @param selector The <code>CriteriaSelector</code> of this obj
	 */
	public HasCriteria(CriteriaSelector selector) {
		this.criteriaSelector = selector;
	}	
	
	/**
	 * Get the <code>CriteriaSelector</code>
	 * @return <code>CriteriaSelector</code> of this obj
	 */
	public CriteriaSelector getSelector() {
		return criteriaSelector;
	}

	/**
	 * Set the <code>CriteriaSelector</code>
	 * @param selector The <code>CriteriaSelector</code> of this obj
	 */
	public void setSelector(CriteriaSelector selector) {
		this.criteriaSelector = selector;
	}
	
    // =========================================================================
    //                  P R O C E S S I N G     M E T H O D S
    // =========================================================================

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }
	
	/**
	 * Deep clone statement to produce a new identical statement.
	 * @return Deep clone 
	 */
	public Object clone() {		
		HasCriteria copy = new HasCriteria();
		copy.setSelector((CriteriaSelector)this.criteriaSelector.clone());
		return copy;
	}
	
    /**
     * Compare two HasCriteria for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the criteriaSelectors are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof HasCriteria)) {
    		return false;
		}

		// Compare the command
		return EquivalenceUtil.areEqual(getSelector(), ((HasCriteria)obj).getSelector());
    } 

    /**
     * Get hashcode for HasCriteria.  WARNING: This hash code relies on the
     * hash codes of the CriteriaSelector on this object. Hash code is only
     * valid after the CriteriaSelector has been set on this object.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the variable and its value for this statement
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.getSelector());
		return myHash;
	}
      
    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }	

} // END CLASS
