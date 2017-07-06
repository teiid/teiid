/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
