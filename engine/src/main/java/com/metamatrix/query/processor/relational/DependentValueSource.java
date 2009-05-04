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

package com.metamatrix.query.processor.relational;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;


/** 
 */
public class DependentValueSource implements
                                 ValueIteratorSource {

    private TupleSourceID tupleSourceID;
    private BufferManager bm;
    private Map<Expression, HashSet<Object>> cachedSets;
    
    public DependentValueSource(TupleSourceID tupleSourceID, BufferManager bm) {
        this.tupleSourceID = tupleSourceID;
        this.bm = bm;
    }
    
    public TupleSourceID getTupleSourceID() {
		return tupleSourceID;
	}
    
    /** 
     * @throws MetaMatrixComponentException 
     * @throws TupleSourceNotFoundException 
     * @see com.metamatrix.query.sql.util.ValueIteratorSource#getValueIterator(com.metamatrix.query.sql.symbol.Expression)
     */
    public ValueIterator getValueIterator(Expression valueExpression) throws  MetaMatrixComponentException {
    	IndexedTupleSource its;
		try {
			its = bm.getTupleSource(tupleSourceID);
		} catch (TupleSourceNotFoundException e) {
			throw new MetaMatrixComponentException(e);
		}
    	int index = 0;
    	if (valueExpression != null) {
    		index = its.getSchema().indexOf(valueExpression);
    		Assertion.assertTrue(index != -1);
    	}
        return new TupleSourceValueIterator(its, index);
    }
    
    public HashSet<Object> getCachedSet(Expression valueExpression) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	HashSet<Object> result = null;
    	if (cachedSets != null) {
    		result = cachedSets.get(valueExpression);
    	}
    	if (result == null) {
    		IndexedTupleSource its;
    		try {
    			if (bm.getRowCount(tupleSourceID) > bm.getProcessorBatchSize() / 2) {
    				return null;
    			}
    			its = bm.getTupleSource(tupleSourceID);
    		} catch (TupleSourceNotFoundException e) {
    			throw new MetaMatrixComponentException(e);
    		}
        	int index = 0;
        	if (valueExpression != null) {
        		index = its.getSchema().indexOf(valueExpression);
        	}
        	Assertion.assertTrue(index != -1);
        	result = new HashSet<Object>();
        	while (its.hasNext()) {
        		Object value = its.nextTuple().get(index);
        		if (value != null) {
        			result.add(value);
        		}
        	}
        	if (cachedSets == null) {
        		cachedSets = new HashMap<Expression, HashSet<Object>>();
        	}
    		cachedSets.put(valueExpression, result);
    	}
    	return result;
    }
           
}
