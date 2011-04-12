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

import java.util.ArrayList;
import java.util.List;

import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.util.CommandContext;

public class ArrayAgg extends AggregateFunction {
	
    private ArrayList<Object> result;
    private CommandContext context;
    
    public ArrayAgg(CommandContext context) {
    	this.context = context;
    }

	@Override
	public void addInputDirect(Object input, List<?> tuple) throws TeiidComponentException, TeiidProcessingException {
		if (this.result == null) {
			this.result = new ArrayList<Object>();
		}
		this.result.add(input);
	}

	@Override
	public Object getResult() throws FunctionExecutionException, ExpressionEvaluationException, TeiidComponentException,TeiidProcessingException {
		if (this.result == null) {
			return null;
		}
		return this.result.toArray();
	}

	@Override
	public void reset() {
		this.result = null;
	}

	@Override
    boolean filter(Object value) {
		// handle the null values too.
    	return false; 
    }	
}
