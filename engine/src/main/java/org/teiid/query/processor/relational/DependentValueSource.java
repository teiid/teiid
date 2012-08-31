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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.teiid.common.buffer.IndexedTupleSource;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.util.Assertion;
import org.teiid.query.sql.symbol.Array;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.util.ValueIteratorSource;



/** 
 */
public class DependentValueSource implements
                                 ValueIteratorSource {

    private TupleBuffer buffer;
    private List<? extends Expression> schema;
    private Map<Expression, Set<Object>> cachedSets;
    private boolean unused; //TODO: use this value instead of the context
    private boolean distinct;

    public DependentValueSource(TupleBuffer tb) {
    	this(tb, tb.getSchema());
    }
    
    public DependentValueSource(TupleBuffer tb, List<? extends Expression> schema) {
        this.buffer = tb;
        this.schema = schema;
    }
    
    public TupleBuffer getTupleBuffer() {
		return buffer;
	}
    
    /** 
     * @throws TeiidComponentException 
     * @see org.teiid.query.sql.util.ValueIteratorSource#getValueIterator(org.teiid.query.sql.symbol.Expression)
     */
    public TupleSourceValueIterator getValueIterator(Expression valueExpression) throws  TeiidComponentException {
    	IndexedTupleSource its = buffer.createIndexedTupleSource();
    	int index = 0;
    	if (valueExpression != null) {
    		if (valueExpression instanceof Array) {
    			final Array array = (Array)valueExpression;
    			List<Expression> exprs = array.getExpressions();
    			final int[] indexes = new int[exprs.size()];
    			for (int i = 0; i < exprs.size(); i++) {
					indexes[i] = getIndex(exprs.get(i));
				}
    	        return new TupleSourceValueIterator(its, index) {
    	        	@Override
    	        	public Object next() throws TeiidComponentException {
    	        		List<?> tuple = super.nextTuple();
    	        		Object[] a = (Object[]) java.lang.reflect.Array.newInstance(array.getComponentType(), indexes.length);
    	        		for (int i = 0; i < indexes.length; i++) {
    	        			a[i] = tuple.get(indexes[i]);
    	        			if (a[i] == null) {
    	        				return null; //TODO: this is a hack
    	        			}
    	        		}
    	        		return new ArrayImpl(a);
    	        	}
    	        };
    		} 
    		index = getIndex(valueExpression);
    	}
        return new TupleSourceValueIterator(its, index);
    }

	private int getIndex(Expression valueExpression) {
		int index = schema.indexOf(valueExpression);
		Assertion.assertTrue(index != -1);
		return index;
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
			TupleSourceValueIterator ve = getValueIterator(valueExpression);
        	int index = 0;
        	Class<?> type = null;
        	if (valueExpression instanceof Array) {
        		type = ((Array)valueExpression).getComponentType();
        	} else { 	
        		if (valueExpression != null) {
	        		index = schema.indexOf(valueExpression);
	            	Assertion.assertTrue(index != -1);
        		}
            	type = ((Expression)schema.get(index)).getType();
        	}

        	if (!DataTypeManager.isHashable(type)) {
        		result = new TreeSet<Object>(Constant.COMPARATOR);
    		} else {
    			result = new HashSet<Object>();
    		}
        	while (ve.hasNext()) {
        		Object value = ve.next();
        		if (value != null) {
        			result.add(value);
        		}
        	}
        	ve.close();
        	if (cachedSets == null) {
        		cachedSets = new HashMap<Expression, Set<Object>>();
        	}
    		cachedSets.put(valueExpression, result);
    	}
    	return result;
    }
    
    @Override
    public boolean isUnused() {
		return unused;
	}
    
    @Override
    public void setUnused(boolean unused) {
		this.unused = unused;
	}
    
    public boolean isDistinct() {
		return distinct;
	}
    
    public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}
           
}
