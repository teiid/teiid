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
