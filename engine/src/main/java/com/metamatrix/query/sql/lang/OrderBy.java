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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.query.QueryPlugin;
import com.metamatrix.query.sql.LanguageObject;
import com.metamatrix.query.sql.LanguageVisitor;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.visitor.SQLStringVisitor;
import com.metamatrix.query.util.ErrorMessageKeys;

/**
 * Represents the ORDER BY clause of a query.  The ORDER BY clause states
 * what order the rows of a result should be returned in.  Each element
 * in the ORDER BY represents a sort that is completed in the order listed.
 * The optional keywords ASC and DESC can be put after each element to
 * specify whether the sort is ascending or descending.
 */
public class OrderBy implements LanguageObject {

	/** Constant for the ascending value */
    public static final boolean ASC = Boolean.TRUE.booleanValue();

	/** Constant for the descending value */
    public static final boolean DESC = Boolean.FALSE.booleanValue();

	private List sortOrder;
    private List orderTypes;
    private boolean inPlanForm = true;
    private boolean hasUnrelated;

    /**
     * Constructs a default instance of this class.
     */
    public OrderBy() {
    	sortOrder = new ArrayList();
        orderTypes = new ArrayList();
    }

    /**
     * Constructs an instance of this class from an ordered set of elements.
     * @param parameters The ordered list of SingleElementSymbol
     */
    public OrderBy( List parameters ) {
        sortOrder = new ArrayList( parameters );
        orderTypes = new ArrayList(parameters.size());
        for( int i=0; i< parameters.size(); i++) {
        	orderTypes.add(Boolean.TRUE);
        }
    }

    /**
     * Constructs an instance of this class from an ordered set of elements.
     * @param parameters The ordered list of SingleElementSymbol
     * @param types The list of directions by which the results are ordered (Boolean, true=ascending)
     */
    public OrderBy( List parameters, List types ) {
        sortOrder = new ArrayList( parameters );
        orderTypes = new ArrayList( types );
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================
    /**
     * Returns the number of elements in ORDER BY.
     * @return Number of variables in ORDER BY
     */
    public int getVariableCount() {
        return sortOrder.size();
    }

    /**
     * Returns an ordered list of the symbols in ORDER BY
     * @param List of SingleElementSymbol in ORDER BY
     */
    public List getVariables() {
        return sortOrder;
    }

    /**
     * Returns an ordered list of sort direction for each order.
     * @param List of Boolean, Boolean.TRUE represents ascending
     */
    public List getTypes() {
        return orderTypes;
    }

    /**
     * Returns the ORDER BY element at the specified index.
     * @param index Index to get
     * @return The element at the index
     */
    public SingleElementSymbol getVariable( int index ) {
        return (SingleElementSymbol)sortOrder.get(index);
    }

    /**
     * Returns the sort order at the specified index
     * @param index Index to get
     * @return The sort order at the index
     */
    public Boolean getOrderType( int index ) {
        return (Boolean)orderTypes.get(index);
    }

    /**
     * Adds a new variable to the list of order by elements.
     * @param element Element to add
     */
    public void addVariable( SingleElementSymbol element ) {
    	if(element != null) {
	        sortOrder.add(element);
            orderTypes.add(Boolean.valueOf(ASC));
        }
    }

    /**
     * Adds a new variable to the list of order by elements with the
     * specified sort order
     * @param element Element to add
     * @param type True for ascending, false for descending
     */
    public void addVariable( SingleElementSymbol element, boolean type ) {
    	if(element != null) {
	        sortOrder.add(element);
            orderTypes.add(Boolean.valueOf(type));
        }
    }
    
    /**
     * Sets a new collection of variables to be used.  The replacement
     * set must be of the same size as the previous set.
     * @param elements Collection of SingleElementSymbol
     * @throws IllegalArgumentException if element is null or size of elements != size of existing elements
     */
    public void replaceVariables( Collection elements ) {
		if(elements == null) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0004));
		}
		if(elements.size() != sortOrder.size()) {
            throw new IllegalArgumentException(QueryPlugin.Util.getString(ErrorMessageKeys.SQL_0005));
		}

        sortOrder = new ArrayList(elements);
    }

    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    // =========================================================================
    //          O V E R R I D D E N     O B J E C T     M E T H O D S
    // =========================================================================

    /**
     * Return deep copy of this ORDER BY clause.
     */
    public Object clone() {
	    List thisSymbols = getVariables();
	    List copySymbols = new ArrayList(thisSymbols.size());
	    Iterator iter = thisSymbols.iterator();
	    while(iter.hasNext()) {
	    	SingleElementSymbol ses = (SingleElementSymbol) iter.next();
	    	copySymbols.add(ses.clone());
	    }
		OrderBy result = new OrderBy(copySymbols, getTypes());
		result.setInPlanForm(this.inPlanForm);
		result.setUnrelated(this.hasUnrelated);
        return result;
	}

	/**
	 * Compare two OrderBys for equality.  Order is important in the order by, so
	 * that is considered in the comparison.  Also, the sort orders are considered.
	 * @param obj Other object
	 * @return True if equal
	 */
	public boolean equals(Object obj) {
		if(obj == this) {
			return true;
		}

		if(!(obj instanceof OrderBy)) {
			return false;
		}

		OrderBy other = (OrderBy) obj;
        return EquivalenceUtil.areEqual(getVariables(), other.getVariables()) &&
               EquivalenceUtil.areEqual(getTypes(), other.getTypes());
	}

	/**
	 * Get hashcode for OrderBy.  WARNING: The hash code relies on the variables
	 * in the select, so changing the variables will change the hash code, causing
	 * a select to be lost in a hash structure.  Do not hash a OrderBy if you plan
	 * to change it.
	 * @return Hash code
	 */
	public int hashCode() {
        int hc = 0;
		hc = HashCodeUtil.hashCode(0, getVariables());
        hc = HashCodeUtil.hashCode(hc, getTypes());
        return hc;
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

    public boolean isInPlanForm() {
        return this.inPlanForm;
    }

    public void setInPlanForm(boolean inPlanForm) {
        this.inPlanForm = inPlanForm;
    }
    
    public boolean hasUnrelated() {
		return hasUnrelated;
	}
    
    public void setUnrelated(boolean hasUnrelated) {
		this.hasUnrelated = hasUnrelated;
	}

}
