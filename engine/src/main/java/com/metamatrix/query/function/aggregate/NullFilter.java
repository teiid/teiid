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

package com.metamatrix.query.function.aggregate;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.query.ExpressionEvaluationException;
import com.metamatrix.api.exception.query.FunctionExecutionException;


/**
 */
public class NullFilter implements AggregateFunction {

    private AggregateFunction proxy;
    
    /**
     * Constructor for NullFilter.
     */
    public NullFilter(AggregateFunction proxy) {
        super();
        
        this.proxy = proxy;
    }

    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#initialize(String, Class)
     */
    public void initialize(Class dataType, Class inputType) {
    	this.proxy.initialize(dataType, inputType);
    }

    public void reset() {
        this.proxy.reset();
    }
    
    /**
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#addInput(Object)
     */
    public void addInput(Object input)
        throws FunctionExecutionException, ExpressionEvaluationException, MetaMatrixComponentException {
        
        if(input != null) { 
            this.proxy.addInput(input);
        } 
    }

    /**
     * @throws MetaMatrixProcessingException 
     * @see com.metamatrix.query.function.aggregate.AggregateFunction#getResult()
     */
    public Object getResult()
        throws MetaMatrixComponentException, MetaMatrixProcessingException {
            
        return this.proxy.getResult();
    }


}
