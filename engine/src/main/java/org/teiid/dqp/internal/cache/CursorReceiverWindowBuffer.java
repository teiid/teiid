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

package org.teiid.dqp.internal.cache;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.metamatrix.common.util.Intervals;

/**
 * Used on the client side of the JDBC driver.  Keeps track of batches of rows retrieved from the server.
 * Removes batches based on the decisions of the ReceiverWindow.
 */
public class CursorReceiverWindowBuffer {
    Intervals bufferIntervals = new Intervals();
    /** Caches the bounding range for the previous call to getRow(). Typically, the next call to getRow() will be lie within these bounds. */
    private int[] currentIntervalsBoundingRange;
    /** Caches the data for the previous call to getRow(). Typically, the next row required will be in this cache. */
    private List[] currentIntervalsData;
    private Map buffer = new HashMap();
    
    public void add(int[] range, List[] data) {
        if (range[1] >= range[0]) {
            bufferIntervals.addInterval(range[0], range[1]);
            buffer.put(new Intervals(range[0], range[1]), data);
        }
    }
    
    public List getRow(int index) {
        if (currentIntervalsBoundingRange != null && currentIntervalsBoundingRange[0] <= index && currentIntervalsBoundingRange[1] >= index) {
            return currentIntervalsData[index-currentIntervalsBoundingRange[0]];
        }
        Iterator iterator = buffer.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry entry = (Map.Entry)iterator.next();
            Intervals i = (Intervals) entry.getKey();
            if (i.containsInterval(index, index)) {
                currentIntervalsData = (List[]) entry.getValue();
                currentIntervalsBoundingRange = i.getBoundingInterval();
                return currentIntervalsData[index-currentIntervalsBoundingRange[0]];
            }
        }
        currentIntervalsData = null;
        currentIntervalsBoundingRange=null;
        throw new IndexOutOfBoundsException();
    }
    
    /* 
     * @see com.metamatrix.common.window.ReceiverWindowBuffer#getContents()
     */
    public Intervals getContents() {
        return bufferIntervals.copy();
    }

    /* 
     * @see com.metamatrix.common.window.ReceiverWindowBuffer#removeFromCache(com.metamatrix.common.util.Intervals)
     */
    public void removeFromCache(Intervals toRemove) {
        if (toRemove.hasIntervals()) {
            Iterator iterator = buffer.keySet().iterator();
            while (iterator.hasNext()) {
                Intervals i = (Intervals) iterator.next();
                int[] range = i.getBoundingInterval();
                if (toRemove.containsInterval(range[0], range[1])) {
                    iterator.remove();
                    bufferIntervals.removeInterval(range[0], range[1]);
                }
            }
        }
    }
     
    public List[] getAllRows() {
    	if (!bufferIntervals.isContiguous()) {
    		throw new IndexOutOfBoundsException();
    	}
    	int baseIndex = bufferIntervals.getBoundingInterval()[0];
    	int size = bufferIntervals.getBoundingInterval()[1] - baseIndex + 1;
    	List[] result = new List[size];
    	for (int i=0; i<size; i++) {    		
    		result[i+baseIndex] = getRow(i);
    	}
    	return result;
    }
    
    
    public boolean containsInterval(int begin, int end) {
        if (currentIntervalsBoundingRange != null &&
            currentIntervalsBoundingRange[0] <= begin &&
            currentIntervalsBoundingRange[1] >= end) {
            return true;
        }
        return bufferIntervals.containsInterval(begin, end);
    }
}
