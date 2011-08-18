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

package org.teiid.query.function.aggregate;

import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;


/**
 * This represents the interface for an aggregate function.  The basic lifecycle
 * is that the AggregateFunction is initialize()'d with the type of the element
 * being aggregated, then addInput() is called for every row in the group, then
 * getResult() is called to retrieve the result.
 */
public abstract class AggregateFunction {

	private int expressionIndex = -1;
	private int conditionIndex = -1;
	
	public void setExpressionIndex(int expressionIndex) {
		this.expressionIndex = expressionIndex;
	}
	
	public void setConditionIndex(int conditionIndex) {
		this.conditionIndex = conditionIndex;
	}

    /**
     * Called to initialize the function.  In the future this may expand
     * with additional information.
     * @param dataType Data type of element begin aggregated
     * @param inputType
     */
    public void initialize(Class<?> dataType, Class<?> inputType) {}

    /**
     * Called to reset the state of the function.
     */
    public abstract void reset();

    public void addInput(List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
    	if (conditionIndex != -1 && !Boolean.TRUE.equals(tuple.get(conditionIndex))) {
			return;
    	}
    	if (expressionIndex == -1) {
    		addInputDirect(null, tuple);
    		return;
    	}
    	Object input = tuple.get(expressionIndex);
    	if (input != null || respectsNull()) {
    		addInputDirect(input, tuple);
    	}
    }
    
    public boolean respectsNull() {
    	return false;
    }
    
    /**
     * Called for the element value in every row of a group.
     * @param input Input value, may be null
     * @param tuple 
     * @throws TeiidProcessingException 
     */
    public abstract void addInputDirect(Object input, List<?> tuple) throws TeiidComponentException, TeiidProcessingException;

    /**
     * Called after all values have been processed to get the result.
     * @return Result value
     * @throws TeiidProcessingException 
     */
    public abstract Object getResult()
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException;

}
