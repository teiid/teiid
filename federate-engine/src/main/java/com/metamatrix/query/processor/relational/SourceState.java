/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.util.Collections;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.common.buffer.IndexedTupleSource;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;

class SourceState {

    private RelationalNode source;
    private BatchCollector collector;
    private IndexedTupleSource tupleSource;
    private TupleSourceID tsID;
    private List<Object> outerVals;
    private TupleSourceIterator iterator;
    private List expressions;
    private int[] expressionIndexes;
    private List currentTuple;
    private int maxProbeMatch = 1;
    private boolean distinct;
    
    public SourceState(RelationalNode source, List expressions) {
        this.source = source;
        List elements = source.getElements();
        this.outerVals = Collections.nCopies(elements.size(), null);
        this.expressionIndexes = getExpressionIndecies(expressions, elements);
    }
    
    private int[] getExpressionIndecies(List expressions,
                                        List elements) {
        if (expressions == null) {
            return new int[0];
        }
        int[] indecies = new int[expressions.size()];
        for (int i = 0; i < expressions.size(); i++) {
            indecies[i] = elements.indexOf(expressions.get(i));
        }
        return indecies;
    }
    
    public List saveNext() throws MetaMatrixComponentException, MetaMatrixProcessingException {
        this.currentTuple = this.getIterator().next();
        return currentTuple;
    }
    
    public void reset() {
        this.getIterator().reset();
        this.getIterator().mark();
        this.currentTuple = null;
    }
    
    public void close() throws TupleSourceNotFoundException, MetaMatrixComponentException {
        closeTupleSource(); 
    }

    private void closeTupleSource() throws TupleSourceNotFoundException,
                                   MetaMatrixComponentException {
        if (this.tsID != null) {
            this.source.getBufferManager().removeTupleSource(tsID);
            this.tsID = null;
        }
    }

    /**
     * Collect the underlying batches into a tuple source.  Subsequent calls will return that tuple source
     */
    public TupleSourceID collectTuples() throws MetaMatrixComponentException,
                               MetaMatrixProcessingException {
        if (collector == null) {
            collector = new BatchCollector(source);
        }
        TupleSourceID result = collector.collectTuples();
        if (this.tsID == null) {
            setTupleSource(result);
        }
        return result;
    }

    void setTupleSource(TupleSourceID result) throws MetaMatrixComponentException, TupleSourceNotFoundException {
        closeTupleSource();
        this.tsID = result;
        try {
            this.tupleSource = source.getBufferManager().getTupleSource(result);
        } catch (TupleSourceNotFoundException e) {
            throw new MetaMatrixComponentException(e, e.getMessage());
        }
    }
    
    TupleSourceIterator getIterator() {
        if (this.iterator == null) {
            if (this.tupleSource != null) {
                iterator = new IndexedTupleSourceIterator(this.tupleSource);
            } else {
                // return a TupleBatch tuplesource iterator
                iterator = new BatchIterator(this.source);
            }
        }
        return this.iterator;
    }

    public List<Object> getOuterVals() {
        return this.outerVals;
    }

    public List getCurrentTuple() {
        return this.currentTuple;
    }

    public int[] getExpressionIndexes() {
        return this.expressionIndexes;
    }
    
    void setMaxProbeMatch(int maxProbeMatch) {
        this.maxProbeMatch = maxProbeMatch;
    }

    int getMaxProbeMatch() {
        return maxProbeMatch;
    }

    public TupleSourceID getTupleSourceID() {
        return this.tsID;
    }

    public boolean isDistinct() {
        return this.distinct;
    }

    public void setDistinct(boolean distinct) {
        this.distinct = distinct;
    }

}
