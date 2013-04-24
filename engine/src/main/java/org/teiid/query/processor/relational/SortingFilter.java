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

import org.teiid.common.buffer.BufferManager;
import org.teiid.common.buffer.BufferManager.TupleSourceType;
import org.teiid.common.buffer.TupleBuffer;
import org.teiid.common.buffer.TupleSource;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.function.aggregate.AggregateFunction;
import org.teiid.query.processor.relational.SortUtility.Mode;
import org.teiid.query.sql.lang.OrderByItem;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.util.CommandContext;

/**
 */
public class SortingFilter extends AggregateFunction {

	// Initial setup - can be reused
    private AggregateFunction proxy;
    private BufferManager mgr;
    private String groupName;
    private boolean removeDuplicates;

    // Derived and static - can be reused
    private List<ElementSymbol> elements;
    private List<OrderByItem> sortItems;
    
    // Temporary state - should be reset
    private TupleBuffer collectionBuffer;
    private SortUtility sortUtility;

    /**
     * Constructor for DuplicateFilter.
     */
    public SortingFilter(AggregateFunction proxy, BufferManager mgr, String groupName, boolean removeDuplicates) {
        super();

        this.proxy = proxy;
        this.mgr = mgr;
        this.groupName = groupName;
        this.removeDuplicates = removeDuplicates;
    }
    
    public List<ElementSymbol> getElements() {
		return elements;
	}
    
	public void setElements(List<ElementSymbol> elements) {
		this.elements = elements;
	}
    
    public void setSortItems(List<OrderByItem> sortItems) {
		this.sortItems = sortItems;
	}
    
    @Override
    public void initialize(java.lang.Class<?> dataType, java.lang.Class<?>[] inputTypes) {
    	this.proxy.initialize(dataType, inputTypes);
    }

    public void reset() {
        this.proxy.reset();
        close();
    }

	private void close() {
		if (this.collectionBuffer != null) {
        	collectionBuffer.remove();
            this.collectionBuffer = null;
        }
		if (this.sortUtility != null) {
			sortUtility.remove();
	        this.sortUtility = null;
		}
	}
	
	@Override
	public void addInputDirect(List<?> tuple, CommandContext commandContext)
			throws TeiidComponentException, TeiidProcessingException {
        if(collectionBuffer == null) {
            collectionBuffer = mgr.createTupleBuffer(elements, groupName, TupleSourceType.PROCESSOR);
        }
        List<Object> row = new ArrayList<Object>(argIndexes.length);
        //TODO remove overlap
        for (int i = 0; i < argIndexes.length; i++) {
			row.add(tuple.get(argIndexes[i]));
		}
        if (!this.proxy.filter(row)) {
            this.collectionBuffer.addTuple(row);
        }
	}
	
    /**
     * @throws TeiidProcessingException 
     * @see org.teiid.query.function.aggregate.AggregateFunction#getResult(CommandContext)
     */
    public Object getResult(CommandContext commandContext)
        throws TeiidComponentException, TeiidProcessingException {

        if(collectionBuffer != null) {
            this.collectionBuffer.close();

            // Sort
            if (sortUtility == null) {
            	sortUtility = new SortUtility(null, sortItems, removeDuplicates?Mode.DUP_REMOVE_SORT:Mode.SORT, mgr, groupName, collectionBuffer.getSchema());
            	collectionBuffer.setForwardOnly(true);
            	this.sortUtility.setWorkingBuffer(collectionBuffer);
            }
            TupleBuffer sorted = sortUtility.sort();
            sorted.setForwardOnly(true);
            try {
	            // Add all input to proxy
	            TupleSource sortedSource = sorted.createIndexedTupleSource();
	            while(true) {
	                List<?> tuple = sortedSource.nextTuple();
	                if(tuple == null) {
	                    break;
	                }
	                //TODO should possibly remove the order by columns from this tuple
	                this.proxy.addInputDirect(tuple, commandContext);
	            }
            } finally {
            	sorted.remove();
            }
            
            close();
        }

        // Return
        return this.proxy.getResult(commandContext);
    }
    
    public boolean respectsNull() {
    	return true;
    }
}
