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

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.buffer.BufferManager.TupleSourceType;
import com.metamatrix.query.function.aggregate.AggregateFunction;
import com.metamatrix.query.sql.lang.OrderBy;
import com.metamatrix.query.sql.symbol.ElementSymbol;
import com.metamatrix.query.util.TypeRetrievalUtil;

/**
 */
public class DuplicateFilter implements AggregateFunction {

    // Initial setup - can be reused
    private AggregateFunction proxy;
    private BufferManager mgr;
    private String groupName;

    // Derived and static - can be reused
    private List elements;
    private String[] elementTypes;
    private List sortTypes;

    // Temporary state - should be reset
    private TupleSourceID collectionID = null;
    private SortUtility sortUtility = null;
    private TupleSourceID sortedID = null;
    private TupleCollector tupleCollector;

    /**
     * Constructor for DuplicateFilter.
     */
    public DuplicateFilter(AggregateFunction proxy, BufferManager mgr, String groupName, int batchSize) {
        super();

        this.proxy = proxy;
        this.mgr = mgr;
        this.groupName = groupName;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#initialize(String, Class)
     */
    public void initialize(Class dataType, Class inputType) {
    	this.proxy.initialize(dataType, inputType);
        // Set up schema
        ElementSymbol element = new ElementSymbol("val"); //$NON-NLS-1$
        element.setType(inputType);
        elements = new ArrayList();
        elements.add(element);
        elementTypes = TypeRetrievalUtil.getTypeNames(elements);

        sortTypes = new ArrayList();
        sortTypes.add(Boolean.valueOf(OrderBy.ASC));
    }

    public void reset() {
        this.proxy.reset();

        this.collectionID = null;
        this.tupleCollector = null;
        this.sortUtility = null;
        this.sortedID = null;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {

        try {
            if(collectionID == null) {
                collectionID = mgr.createTupleSource(elements, elementTypes, groupName, TupleSourceType.PROCESSOR);
                this.tupleCollector = new TupleCollector(collectionID, mgr);
            }

            List row = new ArrayList(1);
            row.add(input);
            this.tupleCollector.addTuple(row);
        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }
    }

    /**
     * @throws MetaMatrixProcessingException 
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult()
        throws MetaMatrixComponentException, MetaMatrixProcessingException {

        try {
            if(collectionID != null) {
                this.tupleCollector.close();

                // Sort
                sortUtility = new SortUtility(collectionID, elements, sortTypes, true, mgr, groupName);
                this.sortedID = sortUtility.sort();

                // Add all input to proxy
                TupleSource sortedSource = mgr.getTupleSource(sortedID);
                while(true) {
                    List tuple = sortedSource.nextTuple();
                    if(tuple == null) {
                        break;
                    }
                    this.proxy.addInput(tuple.get(0));
                }
                sortedSource.closeSource();

                // Clean up
                mgr.removeTupleSource(collectionID);
                mgr.removeTupleSource(sortedID);
            }

        } catch(TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }

        // Return
        return this.proxy.getResult();
    }
}
