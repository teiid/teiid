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
import java.util.Iterator;
import java.util.List;

import org.teiid.core.util.EquivalenceUtil;
import org.teiid.core.util.HashCodeUtil;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.LanguageVisitor;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.visitor.SQLStringVisitor;


/**
 * Represents the ORDER BY clause of a query.  The ORDER BY clause states
 * what order the rows of a result should be returned in.  Each element
 * in the ORDER BY represents a sort that is completed in the order listed.
 * The optional keywords ASC and DESC can be put after each element to
 * specify whether the sort is ascending or descending.
 */
public class OrderBy implements LanguageObject {

	/** Constant for the ascending value */
    public static final boolean ASC = true;

	/** Constant for the descending value */
    public static final boolean DESC = false;

    private List<OrderByItem> orderByItems = new ArrayList<OrderByItem>();
    
    /**
     * Constructs a default instance of this class.
     */
    public OrderBy() {
    }

    /**
     * Constructs an instance of this class from an ordered list of elements.
     * @param parameters The ordered list of SingleElementSymbol
     */
    public OrderBy( List<? extends Expression> parameters ) {
    	for (Expression singleElementSymbol : parameters) {
			orderByItems.add(new OrderByItem(singleElementSymbol, ASC));
		}
    }
    
    /**
     * Constructs an instance of this class from an ordered set of elements.
     * @param parameters The ordered list of SingleElementSymbol
     * @param types The list of directions by which the results are ordered (Boolean, true=ascending)
     */
    public OrderBy( List<? extends Expression> parameters, List<Boolean> types ) {
    	Iterator<Boolean> typeIter = types.iterator();
    	for (Expression singleElementSymbol : parameters) {
			orderByItems.add(new OrderByItem(singleElementSymbol, typeIter.next()));
		}
    }

    // =========================================================================
    //                             M E T H O D S
    // =========================================================================
    /**
     * Returns the number of elements in ORDER BY.
     * @return Number of variables in ORDER BY
     */
    public int getVariableCount() {
        return orderByItems.size();
    }
    
    public List<OrderByItem> getOrderByItems() {
    	return this.orderByItems;
    }

    /**
     * Returns the ORDER BY element at the specified index.
     * @param index Index to get
     * @return The element at the index
     */
    public Expression getVariable( int index ) {
        return orderByItems.get(index).getSymbol();
    }

    /**
     * Returns the sort order at the specified index
     * @param index Index to get
     * @return The sort order at the index
     */
    public Boolean getOrderType( int index ) {
        return orderByItems.get(index).isAscending();
    }

    /**
     * Adds a new variable to the list of order by elements.
     * @param element Element to add
     */
    public void addVariable( Expression element ) {
    	addVariable(element, ASC);
    }

    /**
     * Adds a new variable to the list of order by elements with the
     * specified sort order
     * @param element Element to add
     * @param type True for ascending, false for descending
     */
    public void addVariable( Expression element, boolean type ) {
    	if(element != null) {
            orderByItems.add(new OrderByItem(element, type));
        }
    }
    
    public void acceptVisitor(LanguageVisitor visitor) {
        visitor.visit(this);
    }

    /**
     * Return deep copy of this ORDER BY clause.
     */
    public OrderBy clone() {
    	OrderBy clone = new OrderBy();
    	clone.orderByItems = LanguageObject.Util.deepClone(this.orderByItems, OrderByItem.class);
        return clone;
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
        return EquivalenceUtil.areEqual(orderByItems, other.orderByItems);
	}

	/**
	 * Get hashcode for OrderBy.  WARNING: The hash code relies on the variables
	 * in the select, so changing the variables will change the hash code, causing
	 * a select to be lost in a hash structure.  Do not hash a OrderBy if you plan
	 * to change it.
	 * @return Hash code
	 */
	public int hashCode() {
        return HashCodeUtil.hashCode(0, orderByItems);
	}

    /**
     * Returns a string representation of an instance of this class.
     * @return String representation of object
     */
    public String toString() {
    	return SQLStringVisitor.getSQLString(this);
    }

    public void setExpressionPosition(int orderIndex, int selectIndex) {
    	this.orderByItems.get(orderIndex).setExpressionPosition(selectIndex);
    }
    
    public int getExpressionPosition(int orderIndex) {
    	return this.orderByItems.get(orderIndex).getExpressionPosition();
	}
    
    public void removeOrderByItem(int index) {
        this.orderByItems.remove(index);
    }
    
    public boolean hasUnrelated() {
    	for (OrderByItem item : orderByItems) {
			if (item.isUnrelated()) {
				return true;
			}
		}
    	return false;
    }
    
    /**
     * Get the list or sort key symbols.  Modifications to this list will not add or remove {@link OrderByItem}s.
     * @return
     */
    public List<Expression> getSortKeys() {
    	ArrayList<Expression> result = new ArrayList<Expression>(orderByItems.size());
    	for (OrderByItem item : orderByItems) {
			result.add(item.getSymbol());
		}
    	return result;
    }
    
    public List<Boolean> getTypes() {
    	ArrayList<Boolean> result = new ArrayList<Boolean>(orderByItems.size());
    	for (OrderByItem item : orderByItems) {
			result.add(item.isAscending());
		}
    	return result;
    }
    
}
