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
import com.metamatrix.query.sql.lang.*;
import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;

/**
 * <p>This object represents the criteria used in the stored procedure language
 * that translates the portion of the user's criteria by doing symbol mapping to
 * the elements of the physical group that defines the virtual group and translating
 * the user's criteria using the element-expressions pairs represented as a list of
 * comapreCriteria on this this object.</p>
 */
public class TranslateCriteria extends PredicateCriteria {

	// the selector object used to determine if a type of criteria is specified 
	// on the user's query	
	private CriteriaSelector criteriaSelector;
	
	// List of comparecriteria(element-value pairs) used to translate the user's criteria
	private List translations;
	
	/**
	 * Constructor for TranslateCriteria.
	 */
	public TranslateCriteria() {
	}
	
	/**
	 * Constructor for TranslateCriteria.
	 * @param selector The <code>CriteriaSelector</code> of this obj
	 */
	public TranslateCriteria(CriteriaSelector selector) {
		this.criteriaSelector = selector;
	}
	
	/**
	 * Constructor for TranslateCriteria.
	 * @param selector The <code>CriteriaSelector</code> of this obj
	 * @param critCollect A list of comparecriteria used to translate user's criteria
	 */
	public TranslateCriteria(CriteriaSelector selector, List translations) {
		this.criteriaSelector = selector;
		this.translations = translations;
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

	/**
	 * Return a boolean indicating if the object has any translations.
	 * @return A boolean indicating if the object has any translations
	 */
	public boolean hasTranslations() {
		if(this.translations != null) {
			return (this.translations.size() > 0);			
		}
		return false;
	}

	/**
	 * Set a list of comparecriteria(element-value pairs) used to translate the user's criteria.
	 * @param critCollect A list of criteria used to translate user's criteria
	 */
	public void setTranslations(List translations) {
		this.translations = translations;
	}
	
	/**
	 * Add a comparecriteria(element-value pair) to the list used to translate the user's criteria.
	 * @param criteria A <code>ComapareCriteria</code> object to be added to a collection
	 */
	public void addTranslation(CompareCriteria criteria) {
		if(this.translations == null) {
			this.translations = new ArrayList();	
		}

		this.translations.add(criteria);
	}
	
	/**
	 * Get a list of comparecriteria(element-value pairs) used to translate the user's criteria.
	 * @return A list of criteria used to translate user's criteria
	 */
	public List getTranslations() {
		return this.translations;
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
		TranslateCriteria copy = new TranslateCriteria();
		copy.setSelector((CriteriaSelector)this.criteriaSelector.clone());
		if(this.hasTranslations()) {
			List newTrans = new ArrayList(this.getTranslations().size());
			Iterator transIter = this.getTranslations().iterator();
			while(transIter.hasNext()) {
				Criteria crit = (Criteria) transIter.next();
				newTrans.add(crit);
			}
			copy.setTranslations(newTrans);
		}		
		return copy;
	}
	
    /**
     * Compare two TranslateCriteria for equality.  They will only evaluate to equal if
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
    	if(!(obj instanceof TranslateCriteria)) {
    		return false;
		}

		TranslateCriteria other = (TranslateCriteria) obj;
		
        return 
    		// Compare the selector
            EquivalenceUtil.areEqual(getSelector(), other.getSelector()) &&
            // Compare the selector
            EquivalenceUtil.areEqual(getTranslations(), other.getTranslations());
    }

    /**
     * Get hashcode for TranslateCriteria.  WARNING: This hash code relies on the
     * hash code of the criteria selector on this object. Hash code is only valid
     * after the object has been completely constructed.
     * @return Hash code
     */
    public int hashCode() {
    	// This hash code relies on the variable and its value for this statement
    	// and criteria clauses, not on the from, order by, or option clauses
    	int myHash = 0;
    	myHash = HashCodeUtil.hashCode(myHash, this.getSelector());
    	myHash = HashCodeUtil.hashCode(myHash, this.getTranslations());    	
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
