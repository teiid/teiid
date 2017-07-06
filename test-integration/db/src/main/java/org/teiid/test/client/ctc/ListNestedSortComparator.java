/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.test.client.ctc;

import java.util.List;




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
public class ListNestedSortComparator implements java.util.Comparator, java.io.Serializable {

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
    private List orderTypes = null;
    
    private boolean isDistinct = true;
    private int distinctIndex;

    /**
     * Constructs an instance of this class given the indicies of the parameters
     * to sort on, and whether the sort should be in ascending or descending
     * order.
     */
    public ListNestedSortComparator( int[] sortParameters ) {
        this( sortParameters, false );
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
    public ListNestedSortComparator( int[] sortParameters, List orderTypes ) {
        this.sortParameters = sortParameters;
        this.orderTypes = orderTypes;
    }
    
    public boolean isDistinct() {
		return isDistinct;
	}
    
    public void setDistinctIndex(int distinctIndex) {
		this.distinctIndex = distinctIndex;
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
    public int compare( Object o1, Object o2 ) {
        List list1 = (List)o1;
        List list2 = (List)o2;

        int compare = 0;
        for (int k = 0; k < sortParameters.length; k++) {
            Object param1 = list1.get(sortParameters[k]);
            Object param2 = list2.get(sortParameters[k]);

            if( param1 == null ) {
				if(param2 == null ) {
					// Both are null
					compare = 0;
				} else {
					// param1 = null, so is less than a non-null
					compare = -1;
				}
	    } else if( param2 == null ) {
				// param1 != null, param2 == null
				compare = 1;
	   } else if ( param1 instanceof Comparable ) {
                compare = ((Comparable)param1).compareTo(param2);
            } else {
        	compare = 0;
            }

            if (compare != 0) {
            	boolean asc = orderTypes != null?((Boolean)orderTypes.get(k)).booleanValue():this.ascendingOrder;
                return asc ? compare : -compare;
            } else if (k == distinctIndex) {
        		isDistinct = false;
        	}
        }
    	return 0;
    }
    
} // END CLASS    

