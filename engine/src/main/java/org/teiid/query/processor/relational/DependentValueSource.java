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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.SingleElementSymbol;
import org.teiid.query.sql.util.ValueIterator;
import org.teiid.query.sql.util.ValueIteratorSource;



/** 
 */
public class DependentValueSource implements
                                 ValueIteratorSource {

    private TupleBuffer buffer;
    private Map<Expression, Set<Object>> cachedSets;
    
    public DependentValueSource(TupleBuffer tupleSourceID) {
        this.buffer = tupleSourceID;
    }
    
    public TupleBuffer getTupleBuffer() {
		return buffer;
	}
    
    /** 
     * @throws TeiidComponentException 
     * @see org.teiid.query.sql.util.ValueIteratorSource#getValueIterator(org.teiid.query.sql.symbol.Expression)
     */
    public ValueIterator getValueIterator(Expression valueExpression) throws  TeiidComponentException {
    	IndexedTupleSource its = buffer.createIndexedTupleSource();
    	int index = 0;
    	if (valueExpression != null) {
    		index = buffer.getSchema().indexOf(valueExpression);
    		Assertion.assertTrue(index != -1);
    	}
        return new TupleSourceValueIterator(its, index);
    }
    
    public Set<Object> getCachedSet(Expression valueExpression) throws TeiidComponentException, TeiidProcessingException {
    	Set<Object> result = null;
    	if (cachedSets != null) {
    		result = cachedSets.get(valueExpression);
    	}
    	if (result == null) {
			if (buffer.getRowCount() > buffer.getBatchSize()) {
				return null;
			}
			IndexedTupleSource its = buffer.createIndexedTupleSource();
        	int index = 0;
        	if (valueExpression != null) {
        		index = buffer.getSchema().indexOf(valueExpression);
        	}
        	Assertion.assertTrue(index != -1);
        	if (((SingleElementSymbol)buffer.getSchema().get(index)).getType() == DataTypeManager.DefaultDataClasses.BIG_DECIMAL) {
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
