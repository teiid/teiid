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

package org.teiid.query.function;

import java.util.Map;

import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.api.exception.query.FunctionExecutionException;
import org.teiid.metadata.FunctionMethod.Determinism;
import org.teiid.query.QueryPlugin;
import org.teiid.query.function.metadata.FunctionCategoryConstants;
import org.teiid.query.util.CommandContext;

public class SystemFunctionMethods {
	
	private static final int MAX_VARIABLES = 512;

	@TeiidFunction(category=FunctionCategoryConstants.SYSTEM, nullOnNull=true, determinism=Determinism.COMMAND_DETERMINISTIC)
	public static Object teiid_session_get(CommandContext context, String key) {
		return context.getSessionVariable(key);
	}
	
	@TeiidFunction(category=FunctionCategoryConstants.SYSTEM, determinism=Determinism.COMMAND_DETERMINISTIC)
	public static Object teiid_session_set(CommandContext context, String key, Object value) throws FunctionExecutionException {
		SessionMetadata session = context.getSession();
		Map<String, Object> variables = session.getSessionVariables();
		if (variables.size() > MAX_VARIABLES && !variables.containsKey(key)) {
			throw new FunctionExecutionException(QueryPlugin.Event.TEIID31136, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31136, MAX_VARIABLES));
		}
		return context.setSessionVariable(key, value);
	}

}
