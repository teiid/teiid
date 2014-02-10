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
 
package org.teiid.dqp.internal.process;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.dqp.internal.process.multisource.MultiSourceElement;
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
	private boolean ignoreUnauthorizedAsteriskDefault = PropertiesUtils.getBooleanProperty(System.getProperties(), "org.teiid.ignoreUnauthorizedAsterisk", false); //$NON-NLS-1$
	
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
		if (query.getIsXML() || query.getInto() != null) {
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
	
}
