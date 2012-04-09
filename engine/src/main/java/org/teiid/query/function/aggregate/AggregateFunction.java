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
import org.teiid.query.util.CommandContext;


/**
 * This represents the interface for an aggregate function.  The basic lifecycle
 * is that the AggregateFunction is initialize()'d with the type of the element
 * being aggregated, then addInput() is called for every row in the group, then
 * getResult() is called to retrieve the result.
 */
public abstract class AggregateFunction {

	protected int[] argIndexes;
	private int conditionIndex = -1;
	
	public void setArgIndexes(int[] argIndexes) {
		this.argIndexes = argIndexes;
	}
	
	public void setConditionIndex(int conditionIndex) {
		this.conditionIndex = conditionIndex;
	}

    /**
     * Called to initialize the function.  In the future this may expand
     * with additional information.
     * @param dataType Data type of element begin aggregated
     * @param inputTypes
     */
    public void initialize(Class<?> dataType, Class<?>[] inputTypes) {}
    
    public int[] getArgIndexes() {
		return argIndexes;
	}

    /**
     * Called to reset the state of the function.
     */
    public abstract void reset();

    public void addInput(List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException {
    	if (conditionIndex != -1 && !Boolean.TRUE.equals(tuple.get(conditionIndex))) {
			return;
    	}
    	if (filter(tuple)) {
    		return;
    	}
		addInputDirect(tuple, commandContext);
    }

	public boolean filter(List<?> tuple) {
		if (!respectsNull()) {
    		for (int i = 0; i < argIndexes.length; i++) {
    			if (tuple.get(argIndexes[i]) == null) {
    				return true;
    			}
    		}
    	}
		return false;
	}
    
    public boolean respectsNull() {
    	return false;
    }
    
    /**
     * Called for the element value in every row of a group.
     * @param tuple 
     * @param commandContext
     * @throws TeiidProcessingException 
     */
    public abstract void addInputDirect(List<?> tuple, CommandContext commandContext) throws TeiidComponentException, TeiidProcessingException;

    /**
     * Called after all values have been processed to get the result.
     * @param commandContext
     * @return Result value
     * @throws TeiidProcessingException 
     */
    public abstract Object getResult(CommandContext commandContext)
        throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException, TeiidProcessingException;

}
