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
import java.util.Set;
import java.util.TreeSet;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleBuffer;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.core.util.Assertion;
import com.metamatrix.query.sql.symbol.Expression;
import com.metamatrix.query.sql.symbol.SingleElementSymbol;
import com.metamatrix.query.sql.util.ValueIterator;
import com.metamatrix.query.sql.util.ValueIteratorSource;


/** 
 */
public class DependentValueSource implements
                                 ValueIteratorSource {

    private TupleBuffer tupleSourceID;
    private int maxSetSize;
    private Map<Expression, Set<Object>> cachedSets;
    
    public DependentValueSource(TupleBuffer tupleSourceID, int maxSetSize) {
        this.tupleSourceID = tupleSourceID;
        this.maxSetSize = maxSetSize;
    }
    
    public TupleBuffer getTupleBuffer() {
		return tupleSourceID;
	}
    
    /** 
     * @throws MetaMatrixComponentException 
     * @see com.metamatrix.query.sql.util.ValueIteratorSource#getValueIterator(com.metamatrix.query.sql.symbol.Expression)
     */
    public ValueIterator getValueIterator(Expression valueExpression) throws  MetaMatrixComponentException {
    	IndexedTupleSource its = tupleSourceID.createIndexedTupleSource();
    	int index = 0;
    	if (valueExpression != null) {
    		index = its.getSchema().indexOf(valueExpression);
    		Assertion.assertTrue(index != -1);
    	}
        return new TupleSourceValueIterator(its, index);
    }
    
    public Set<Object> getCachedSet(Expression valueExpression) throws MetaMatrixComponentException, MetaMatrixProcessingException {
    	Set<Object> result = null;
    	if (cachedSets != null) {
    		result = cachedSets.get(valueExpression);
    	}
    	if (result == null) {
			if (tupleSourceID.getRowCount() > maxSetSize) {
				return null;
			}
			IndexedTupleSource its = tupleSourceID.createIndexedTupleSource();
        	int index = 0;
        	if (valueExpression != null) {
        		index = its.getSchema().indexOf(valueExpression);
        	}
        	Assertion.assertTrue(index != -1);
        	if (((SingleElementSymbol)its.getSchema().get(index)).getType() == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
        		result = new TreeSet<Object>();
    		} else {
    			result = new HashSet<Object>();
    		}
        	while (its.hasNext()) {
        		Object value = its.nextTuple().get(index);
        		if (value != null) {
        			result.add(value);
        		}
        	}
        	its.closeSource();
        	if (cachedSets == null) {
        		cachedSets = new HashMap<Expression, Set<Object>>();
        	}
    		cachedSets.put(valueExpression, result);
    	}
    	return result;
    }
           
}
