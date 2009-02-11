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

package com.metamatrix.query.function.aggregate;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;

/**
 * This represents the interface for an aggregate function.  The basic lifecycle
 * is that the AggregateFunction is initialize()'d with the type of the element
 * being aggregated, then addInput() is called for every row in the group, then
 * getResult() is called to retrieve the result.
 */
public interface AggregateFunction {


    /**
     * Called to initialize the function.  In the future this may expand
     * with additional information.
     * @param dataType Data type of element begin aggregated
     */
    public abstract void initialize(Class dataType);

    /**
     * Called to reset the state of the function.
     */
    public abstract void reset();

    /**
     * Called for the element value in every row of a group.
     * @param input Input value, may be null
     */
    public abstract void addInput(Object input) 
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException;


    /**
     * Called after all values have been processed to get the result.
     * @return Result value
     * @throws MetaMatrixProcessingException 
     */
    public abstract Object getResult()
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException, MetaMatrixProcessingException;

}
