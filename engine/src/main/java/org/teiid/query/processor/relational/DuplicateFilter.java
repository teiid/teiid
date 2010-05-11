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

import java.util.ArrayList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderBy;
import org.teiid.query.sql.symbol.ElementSymbol;


/**
 */
public class DuplicateFilter implements AggregateFunction {

    // Initial setup - can be reused
    private AggregateFunction proxy;
    private BufferManager mgr;
    private String groupName;

    // Derived and static - can be reused
    private List elements;
    private List sortTypes;

    // Temporary state - should be reset
    private TupleBuffer collectionBuffer;
    private SortUtility sortUtility;

    /**
     * Constructor for DuplicateFilter.
     */
    public DuplicateFilter(AggregateFunction proxy, BufferManager mgr, String groupName) {
        super();

        this.proxy = proxy;
        this.mgr = mgr;
        this.groupName = groupName;
    }
    
    public List getElements() {
		return elements;
	}

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#initialize(String, Class)
     */
    public void initialize(Class dataType, Class inputType) {
    	this.proxy.initialize(dataType, inputType);
        // Set up schema
        ElementSymbol element = new ElementSymbol("val"); //$NON-NLS-1$
        element.setType(inputType);
        elements = new ArrayList();
        elements.add(element);

        sortTypes = new ArrayList();
        sortTypes.add(Boolean.valueOf(OrderBy.ASC));
    }

    public void reset() {
        this.proxy.reset();
        close();
    }

	private void close() {
		if (this.collectionBuffer != null) {
        	collectionBuffer.remove();
        }
        this.collectionBuffer = null;
        this.sortUtility = null;
	}

    /**
     * @see org.teiid.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException {

        if(collectionBuffer == null) {
            collectionBuffer = mgr.createTupleBuffer(elements, groupName, TupleSourceType.PROCESSOR);
            collectionBuffer.setForwardOnly(true);
        }

        List row = new ArrayList(1);
        row.add(input);
        this.collectionBuffer.addTuple(row);
    }

    /**
     * @throws TeiidProcessingException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult()
        throws TeiidComponentException, TeiidProcessingException {

        if(collectionBuffer != null) {
            this.collectionBuffer.close();

            // Sort
            if (sortUtility == null) {
            	sortUtility = new SortUtility(collectionBuffer.createIndexedTupleSource(), elements, sortTypes, Mode.DUP_REMOVE_SORT, mgr, groupName);
            }
            TupleBuffer sorted = sortUtility.sort();
            sorted.setForwardOnly(true);
            try {
	            // Add all input to proxy
	            TupleSource sortedSource = sorted.createIndexedTupleSource();
	            while(true) {
	                List tuple = sortedSource.nextTuple();
	                if(tuple == null) {
	                    break;
	                }
	                this.proxy.addInput(tuple.get(0));
	            }
            } finally {
            	sorted.remove();
            }
            
            close();
        }

        // Return
        return this.proxy.getResult();
    }
}
