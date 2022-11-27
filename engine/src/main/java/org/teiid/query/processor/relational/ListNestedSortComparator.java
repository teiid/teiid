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

package org.teiid.query.processor.relational;

import java.util.Collections;
import java.util.List;

import org.teiid.language.SortSpecification.NullOrdering;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.translator.ExecutionFactory.NullOrder;

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

    private boolean init;
    private int nullValue = -1;

    private NullOrder defaultNullOrder = NullOrder.LOW;

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

    public ListNestedSortComparator<T> defaultNullOrder(NullOrder order) {
        this.defaultNullOrder = order;
        return this;
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
     * @param list1 The first object being compared
     * @param list2 The second object being compared
     */

    public int compare(java.util.List<T> list1, java.util.List<T> list2) {
        if (!init) {
            if (nullOrdering == null) {
                nullOrdering = Collections.nCopies(sortParameters.length, null);
            }
            for (int i = 0; i < sortParameters.length; i++) {
                if (nullOrdering.get(i) == null) {
                    if (defaultNullOrder == NullOrder.FIRST) {
                        nullOrdering.set(i, NullOrdering.FIRST);
                    } else if (defaultNullOrder == NullOrder.LAST) {
                        nullOrdering.set(i, NullOrdering.LAST);
                    }
                }
            }
            if (defaultNullOrder == NullOrder.HIGH) {
                nullValue = 1;
            }
            init = true;
        }
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
                    compare = nullValue;
                    NullOrdering no = getNullOrdering(k);
                    if (no == NullOrdering.FIRST) {
                        return -1;
                    }
                    if (no == NullOrdering.LAST) {
                        return 1;
                    }
                }
            } else if( param2 == null ) {
                // param1 != null, param2 == null
                compare = -nullValue;
                NullOrdering no = getNullOrdering(k);
                if (no == NullOrdering.FIRST) {
                    return 1;
                }
                if (no == NullOrdering.LAST) {
                    return -1;
                }
            } else  {
                compare = Constant.COMPARATOR.compare(param1, param2);
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
        return nullOrdering.get(index);
    }

    public int[] getSortParameters() {
        return sortParameters;
    }

    public void setSortParameters(int[] sortParameters) {
        this.sortParameters = sortParameters;
    }

    public List<Boolean> getOrderTypes() {
        return orderTypes;
    }

    public void setOrderTypes(List<Boolean> orderTypes) {
        this.orderTypes = orderTypes;
    }

} // END CLASS

