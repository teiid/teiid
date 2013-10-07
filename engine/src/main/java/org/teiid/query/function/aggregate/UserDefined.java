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

import org.teiid.UserDefinedAggregate;
import org.teiid.api.exception.query.ExpressionEvaluationException;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.function.FunctionDescriptor;
import org.teiid.query.util.CommandContext;

public class UserDefined extends AggregateFunction {
	
	private FunctionDescriptor fd;
	private UserDefinedAggregate<?> instance;
	private Object[] values;
	
	public UserDefined(FunctionDescriptor functionDescriptor) {
		this.fd = functionDescriptor;
		this.instance = (UserDefinedAggregate<?>) fd.newInstance();
	}

	@Override
	public void addInputDirect(List<?> tuple, CommandContext commandContext) throws TeiidComponentException,
			TeiidProcessingException {
		if (values == null) {
			values = new Object[argIndexes.length + (fd.requiresContext()?1:0)];
		}
		if (fd.requiresContext()) {
			values[0] = commandContext;
		}
		for (int i = 0; i < argIndexes.length; i++) {
			values[i + (fd.requiresContext()?1:0)] = tuple.get(argIndexes[i]);
		}
		fd.invokeFunction(values, commandContext, instance);
	}
	
	@Override
	public void reset() {
		instance.reset();
	}
	
	@Override
	public Object getResult(CommandContext commandContext) throws FunctionExecutionException,
			ExpressionEvaluationException, TeiidComponentException,
			TeiidProcessingException {
		return instance.getResult(commandContext);
	}
	
	@Override
	public boolean respectsNull() {
		return !fd.getMethod().isNullOnNull();
	}
	
}
