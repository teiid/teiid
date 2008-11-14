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

import java.util.ArrayList;
import java.util.List;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;
import com.metamatrix.query.function.aggregate.AggregateFunction;

/**
 */
public class AggregateFunctionCollector implements AggregateFunction {

    private List collectedValues = new ArrayList();

    /**
     * Constructor for AggregateFunctionCollector.
     */
    public AggregateFunctionCollector() {
        super();
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#initialize(String)
     */
    public void initialize(Class dataType) {
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#reset()
     */
    public void reset() {
        this.collectedValues = new ArrayList();
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {
            
        this.collectedValues.add(input);
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult()
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {

        return this.collectedValues;            
    }

}
