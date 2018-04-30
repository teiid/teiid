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
 
package org.teiid.dqp.internal.process;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.CoreConstants;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.query.QueryPlugin;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TempMetadataID;
import org.teiid.query.sql.LanguageObject;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.Expression;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.util.CommandContext;

/**
 * The default Teiid authorization validator
 */
public class DefaultAuthorizationValidator implements AuthorizationValidator {
	
	public static final String IGNORE_UNAUTHORIZED_ASTERISK = "ignore_unauthorized_asterisk"; //$NON-NLS-1$
	private PolicyDecider policyDecider;
	private boolean ignoreUnauthorizedAsteriskDefault = PropertiesUtils.getHierarchicalProperty("org.teiid.ignoreUnauthorizedAsterisk", false, Boolean.class); //$NON-NLS-1$
	
	public DefaultAuthorizationValidator() {
	}

	@Override
	public boolean validate(String[] originalSql, Command command,
			QueryMetadataInterface metadata, CommandContext commandContext,
			CommandType commandType) throws QueryValidatorException,
			TeiidComponentException {
		boolean modified = false;
		if (policyDecider != null && policyDecider.validateCommand(commandContext)) {
			if (ignoreUnathorizedInAsterisk(command, commandContext)) {
				Query query = (Query)command;
				HashMap<String, LanguageObject> map = null;
        		for (Expression ex : query.getSelect().getSymbols()) {
        			if (ex instanceof MultipleElementSymbol) {
        				MultipleElementSymbol mes = (MultipleElementSymbol)ex;
        				if (map == null) {
        					map = new HashMap<String, LanguageObject>();
        				}
        				for (Iterator<ElementSymbol> iter = mes.getElementSymbols().iterator(); iter.hasNext();) {
        					ElementSymbol es = iter.next();
        					Object metadataObject = es.getMetadataID();
        					if (metadataObject instanceof MultiSourceElement || metadataObject instanceof TempMetadataID) {
        						continue;
        					}
        					map.clear();
        					AuthorizationValidationVisitor.addToNameMap(metadataObject, es, map, commandContext.getMetadata());
        					Set<String> results = this.policyDecider.getInaccessibleResources(PermissionType.READ, map.keySet(), Context.QUERY, commandContext);
        					if (!results.isEmpty()) {
        						iter.remove(); //remove from the select
        						modified = true;
        					}
        				}
        			}
        		}
        		if (query.getProjectedSymbols().isEmpty()) {
        			throw new QueryValidatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31151));
        		}
			}
			AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(this.policyDecider, commandContext);
			Request.validateWithVisitor(visitor, metadata, command);
		}		
		return modified;
	}

	private boolean ignoreUnathorizedInAsterisk(Command command, CommandContext commandContext) {
		if (!(command instanceof Query)) {
			return false;
		}
		Query query = (Query)command;
		if (query.getInto() != null) {
			return false;
		}
		if (ignoreUnauthorizedAsteriskDefault) {
			return true;
		}
		Object value = commandContext.getSessionVariable(IGNORE_UNAUTHORIZED_ASTERISK);
		if (value != null) {
			try {
				return Boolean.TRUE.equals(DataTypeManager.transformValue(value, DataTypeManager.DefaultDataClasses.BOOLEAN));
			} catch (TransformationException e) {
			}
		}
		return false;
	}
	
	@Override
	public boolean hasRole(String roleName, CommandContext commandContext) {
		if (policyDecider == null) {
			return true;
		}
		return this.policyDecider.hasRole(roleName, commandContext);
	}
	
	public void setPolicyDecider(PolicyDecider policyDecider) {
		this.policyDecider = policyDecider;
	}
	
	@Override
	public boolean isAccessible(AbstractMetadataRecord record,
			CommandContext commandContext) {
		if (policyDecider == null || !policyDecider.validateCommand(commandContext) 
				//TODO - schemas cannot be hidden - unless we traverse them and find that nothing is accessible
				|| record instanceof Schema) {
			return true;
		}
		AbstractMetadataRecord parent = record;
		while (parent.getParent() != null) {
			parent = parent.getParent();
			if (parent instanceof Procedure) {
				return true; //don't check procedure params/rs columns
			}
		}
		if (!(parent instanceof Schema) || (CoreConstants.SYSTEM_MODEL.equalsIgnoreCase(parent.getName()) || CoreConstants.ODBC_MODEL.equalsIgnoreCase(parent.getName()))) {
			//access is always allowed to system tables / procedures or unrooted objects
			return true;
		}
		PermissionType action = PermissionType.READ;
		if (record instanceof FunctionMethod || record instanceof Procedure) {
			action = PermissionType.EXECUTE;
		}
		
		//cache permission check
		Boolean result = commandContext.isAccessible(record);
		if (result != null) {
			return result;
		}
		
		HashSet<String> resources = new HashSet<String>(2);
		resources.add(record.getFullName());
		result = this.policyDecider.getInaccessibleResources(action, resources, Context.METADATA, commandContext).isEmpty();
		commandContext.setAccessible(record, result);
		return result;
	}
	
}
