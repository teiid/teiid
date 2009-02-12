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

package com.metamatrix.common.buffer.impl;

import java.util.Comparator;

/**
 * Comparator that compares batches based on their begin row.
 * Just a handy convenience.
 */
class BatchComparator implements Comparator {

    /**
     * Constructor for BatchComparator.
     */
    public BatchComparator() {
        super();
    }

    /**
     * Compare two TupleBatch objects and return comparison value
     * based on the begin rows of the batches
     * @see java.util.Comparator#compare(Object, Object)
     * @param o1 First TupleBatch
     * @param o2 Second TupleBatch
     * @return -1, 0, or 1 as o1 compares to o2
     */
    public int compare(Object o1, Object o2) {
        if(o1 == null) { 
            return -1;
        } else if(o2 == null) { 
            return 1;
        }
        
        ManagedBatch batch1 = (ManagedBatch) o1;
        ManagedBatch batch2 = (ManagedBatch) o2;
        
        long last1 = batch1.getLastAccessed();
        long last2 = batch2.getLastAccessed();
        
        if(last1 < last2) {
            return -1;
        } else if(last1 > last2) {
            return 1;
        } else {
            return 0;
        }
    }

}
