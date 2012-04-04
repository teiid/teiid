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

import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.util.CommandContext;

public abstract class SingleArgumentAggregateFunction extends AggregateFunction {

	@Override
	public void addInputDirect(List<?> tuple, CommandContext commandContext)
			throws TeiidComponentException, TeiidProcessingException {
		addInputDirect(tuple.get(argIndexes[0]), tuple, commandContext);
	}
	
	public void initialize(java.lang.Class<?> dataType, java.lang.Class<?>[] inputTypes) {
		initialize(dataType, inputTypes[0]);
	}
	
	/**
	 * @param dataType  
	 * @param inputType 
	 */
	public void initialize(java.lang.Class<?> dataType, java.lang.Class<?> inputType) {
		
	}
	
	public abstract void addInputDirect(Object input, List<?> tuple, CommandContext commandContext)
    throws TeiidProcessingException, TeiidComponentException;
}
