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

import java.util.*;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.sql.*;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p>This class represents the criteria present on the user's query. The type of
 * criteria and the elements on which it is specified are listed in this object,
 * this object is used by <code>HasCriteria</code> and <code>TranslateCriteria</code>
 * objects to determine if a particular type of criteria is present on one or more
 * elements on a user's query</p>
 */
public class CriteriaSelector implements LanguageObject {
	
	// constant for "=" criteria 
	public static final int COMPARE_EQ = CompareCriteria.EQ;

	// constant for "<>" criteria 	
	public static final int COMPARE_NE = CompareCriteria.NE;
	
	// constant for "<" criteria 	
	public static final int COMPARE_LT = CompareCriteria.LT;
	
	// constant for ">" criteria 
	public static final int COMPARE_GT = CompareCriteria.GT;
	
	// constant for "<=" criteria 	
	public static final int COMPARE_LE = CompareCriteria.LE;
	
	// constant for ">=" criteria 	
	public static final int COMPARE_GE = CompareCriteria.GE;
	
	// constant for "LIKE" criteria 	
	public static final int LIKE = 7;
	
    // constant for "IN" criteria   
    public static final int IN = 8;
    
	// constant for "IS NULL" criteria 	
	public static final int IS_NULL = 9;
	
    // constant for "BETWEEN" criteria   
    public static final int BETWEEN = 10;
    
	// constant for ">=" criteria 	
	public static final int NO_TYPE = 0;	
	
	// type of criteria
	private int selectorType = NO_TYPE;

	// elements on which criteria is present
	private List elements;

	/**
	 * Constructor for CriteriaSelector.
	 */
	public CriteriaSelector() {
		super();
	}
	
	/**
	 * Constructor for CriteriaSelector.
	 * @param selectorType The type criteria presents on the elements
	 * @param elements The elements on which 
	 */
	public CriteriaSelector(int selectorType, List elements) {
		this.selectorType = selectorType;
		this.elements = elements;
	}

	/**
	 * Get the type of criteria on the user query's elements
	 * @return An int value giving the type of criteria
	 */
	public int getSelectorType() {
		return this.selectorType;
	}
	
	/**
	 * Set the type of criteria on the user query's elements
	 * @param type The type of criteria
	 */	
	public void setSelectorType(int type) {
		this.selectorType = type;
	}	

	/**
	 * Get elements on which criteria is pecified on the user's query
	 * @return A collection of elements used in criteria
	 */
	public List getElements() {
		return this.elements;
	}

	/**
	 * Set elements on which criteria is pecified on the user's query
	 * @param elements A collection of elements used in criteria
	 */	
	public void setElements(List elements) {
		this.elements = elements;
	}
	
	/**
	 * Add an element to the collection of elements on which
	 * criteria is pecified on the user's query
	 * @param element The elementSymbol object being added
	 */	
	public void addElement(ElementSymbol element) {
		if(elements == null) {
			elements = new ArrayList();
		}
		elements.add(element);
	}
	
	/**
	 * Return a boolean indicating if the seletor has any elements
	 * @return A boolean indicating if the seletor has any elements
	 */
	public boolean hasElements() {
		return (elements != null && !elements.isEmpty());
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
		CriteriaSelector copy = new CriteriaSelector();
		
		copy.setSelectorType(this.selectorType);		
		if(this.hasElements()) {
			Iterator stmtIter = this.elements.iterator();
			while(stmtIter.hasNext()) {
				copy.addElement((ElementSymbol)((ElementSymbol) stmtIter.next()).clone());
			}
		}	
		return copy;
	}
	
    /**
     * Compare two CriteriaSelector for equality.  They will only evaluate to equal if
     * they are IDENTICAL: the selectorType and elements present are equal.
     * @param obj Other object
     * @return True if equal
     */
    public boolean equals(Object obj) {
    	// Quick same object test
    	if(this == obj) {
    		return true;
		}

		// Quick fail tests		
    	if(!(obj instanceof CriteriaSelector)) {
    		return false;
		}

		CriteriaSelector other = (CriteriaSelector) obj;

        return 
    		// Compare the selector type		
    		getSelectorType() == other.getSelectorType() &&
    		// Compare the elements
    		EquivalenceUtil.areEqual(getElements(), other.getElements());
    }

    /**
     * Get hashcode for CriteriaSelector.  WARNING: This hash code relies on the
     * hash codes of the elements and the selectortype on this object. Hash code is only valid after
     * the block has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the variable and its value for this statement
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = 0;
		if(hasElements()) {
	    	myHash = HashCodeUtil.hashCode(myHash, this.getElements());
		}
    	myHash = HashCodeUtil.hashCode(myHash, this.getSelectorType());		
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
