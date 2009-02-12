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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;


/** 
 * This class is used to save batches ordered by the first row.
 * @since 4.3
 */
public class BatchMap {
    private TreeMap batches;
    private BatchMapValueTranslator translator;

    public BatchMap(BatchMapValueTranslator translator) {
        batches = new TreeMap();
        this.translator = translator;
    }
    
    public void addBatch(int beginRow, Object batch) {
        batches.put(new Integer(beginRow), batch);
    }
    
    public Object getBatch(int beginRow) {
        
        //check special cases first
        int batchSize = batches.size();
        if(batchSize == 0) {
            return null;
        }
        Object batch;
        if(batchSize == 1) {
            batch = batches.values().iterator().next();
            if(beginRow >= translator.getBeginRow(batch) && beginRow <= translator.getEndRow(batch)) {
                return batch;
            }
            return null;
        }
        
        //Try to search by beginRow. This is fast.
        if((batch = batches.get(new Integer(beginRow))) != null) {
            return batch;
        }

        List batchList = new ArrayList(batches.values());       
        return doBinarySearchForBatch(batchList, beginRow);
    }
    
    public void removeBatch(int beginRow) {
        if(batches.isEmpty()) {
            return;
        }
        batches.remove(new Integer(beginRow));
    }
    
    public Iterator getBatchIterator() {
        return batches.values().iterator();
    }
    
    public boolean isEmpty() {
        return batches.isEmpty();
    }
    
    private Object doBinarySearchForBatch(List batches, int beginRow) {
        int batchSize = batches.size();
        int beginIndex = 0;
        int midIndex;
        int endIndex = batchSize;
        Object batch;
        while(beginIndex < endIndex) {
            midIndex = (beginIndex + endIndex)/2;
            batch = batches.get(midIndex);
            if(beginRow < translator.getBeginRow(batch)){
                endIndex = midIndex;
            }else if(beginRow > translator.getEndRow(batch)) {
                beginIndex = midIndex + 1;
            }else {
                return batch;
            }
        }
        return null;
    }
}
