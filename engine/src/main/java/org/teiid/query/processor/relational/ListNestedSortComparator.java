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

package org.teiid.query.processor.relational;

import java.util.List;

import org.teiid.language.SortSpecification.NullOrdering;

/**
 * This class can be used for comparing lists of elements, when the fields to
 * be sorted on and the comparison mechanism are dynamically specified. <p>
 *
 * Typically, the lists are records in a collection that is to be sorted. <p>
 *
 * <h3>Example</h3>
 * <pre>
 *    Records...
 *      { "a1", "b1", "c1" } 
 *      { "a1", "b1", "c2" }
 *      { "a1", "b2", "c1" }
 *      { "a1", "b2", "c2" }
 *      { "a2", "b1", "c1" } 
 *      { "a2", "b1", "c2" } 
 *      { "a2", "b2", "c1" } 
 *      { "a2", "b2", "c2" }
 *
 *    Records sorted in ascending order on columns 0, 2...
 *      { "a1", "b1", "c1" } 
 *      { "a1", "b2", "c1" }
 *      { "a1", "b2", "c2" }
 *      { "a1", "b1", "c2" }
 *      { "a2", "b1", "c1" } 
 *      { "a2", "b2", "c1" } 
 *      { "a2", "b1", "c2" } 
 *      { "a2", "b2", "c2" } 
 * </pre>
 */
public class ListNestedSortComparator<T extends Comparable<? super T>> implements java.util.Comparator<List<T>> {

    /**
     * Specifies which fields to sort on.
     */
    private int[] sortParameters;

    /**
     * Indicates whether comparison should be based on ascending or descending
     * order.
     */
    private boolean ascendingOrder = false;

    /**
     * List of booleans indicating the order in which each column should be sorted
     */
    private List<Boolean> orderTypes = null;
    
    private boolean isDistinct = true;
    private int distinctIndex;
    
    private List<NullOrdering> nullOrdering;

    /**
     * Constructs an instance of this class given the indicies of the parameters
     * to sort on, and whether the sort should be in ascending or descending
     * order.
     */
    public ListNestedSortComparator( int[] sortParameters ) {
        this( sortParameters, true );
    }

    /**
     * Constructs an instance of this class given the indicies of the parameters
     * to sort on, and whether the sort should be in ascending or descending
     * order.
     */
    public ListNestedSortComparator( int[] sortParameters, boolean ascending ) {
        this.sortParameters = sortParameters;
        this.ascendingOrder = ascending;
    }

    /**
     * Constructs an instance of this class given the indicies of the parameters
     * to sort on, and orderList used to determine the order in which each column
     * is sorted.
     */
    public ListNestedSortComparator( int[] sortParameters, List<Boolean> orderTypes ) {
        this.sortParameters = sortParameters;
        this.orderTypes = orderTypes;
    }
    
    public boolean isDistinct() {
		return isDistinct;
	}
    
    public void setDistinctIndex(int distinctIndex) {
		this.distinctIndex = distinctIndex;
	}
    
    public void setNullOrdering(List<NullOrdering> nullOrdering) {
		this.nullOrdering = nullOrdering;
	}

    /**
     * Compares its two arguments for order.  Returns a negative integer,
     * zero, or a positive integer as the first argument is less than,
     * equal to, or greater than the second. <p>
     *
     * The <code>compare</code> method returns <p>
     * <ul>
     *      <li>-1 if object1 less than object 2 </li>
     *      <li> 0 if object1 equal to object 2 </li>
     *      <li>+1 if object1 greater than object 2 </li>
     * </ul>
     *
     * @param o1 The first object being compared
     * @param o2 The second object being compared
     */
    
    public int compare(java.util.List<T> list1, java.util.List<T> list2) {
        int compare = 0;
        for (int k = 0; k < sortParameters.length; k++) {
        	if (list1.size() <= sortParameters[k]) {
            	return 1;
            }
            T param1 = list1.get(sortParameters[k]);
            if (list2.size() <= sortParameters[k]) {
            	return -1;
            }
            T param2 = list2.get(sortParameters[k]);

            if( param1 == null ) {
				if(param2 == null ) {
					// Both are null
					compare = 0;
				} else {
					// param1 = null, so is less than a non-null
					compare = -1;
					NullOrdering no = getNullOrdering(k);
					if (no != null) {
						if (nullOrdering.get(k) == NullOrdering.FIRST) {
							return -1;
						} 
						return 1;
					}
				}
            } else if( param2 == null ) {
				// param1 != null, param2 == null
				compare = 1;
				NullOrdering no = getNullOrdering(k);
				if (no != null) {
					if (nullOrdering.get(k) == NullOrdering.FIRST) {
						return 1;
					} 
					return -1;
				}
            } else  {
                compare = param1.compareTo(param2);
            } 
            if (compare != 0) {
            	boolean asc = orderTypes != null?orderTypes.get(k):this.ascendingOrder;
                return asc ? compare : -compare;
            } else if (k == distinctIndex) {
        		isDistinct = false;
        	}
        }
    	return 0;
    }
    
    private NullOrdering getNullOrdering(int index) {
    	if (nullOrdering != null) {
    		return nullOrdering.get(index);
    	}
    	return null;	
    }
    
    public int[] getSortParameters() {
		return sortParameters;
	}
    
    public void setSortParameters(int[] sortParameters) {
		this.sortParameters = sortParameters;
	}
    
} // END CLASS    

