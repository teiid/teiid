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

import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;

/**
 * The default Teiid authorization validator
 */
public class DataRoleAuthorizationValidator implements AuthorizationValidator {
	
	private boolean useEntitlements;
	private boolean allowCreateTemporaryTablesByDefault;
	
	public DataRoleAuthorizationValidator(boolean useEntitlements,
			boolean allowCreateTemporaryTablesByDefault) {
		this.useEntitlements = useEntitlements;
		this.allowCreateTemporaryTablesByDefault = allowCreateTemporaryTablesByDefault;
	}

	@Override
	public void validate(Command command, QueryMetadataInterface metadata, DQPWorkContext workContext) throws QueryValidatorException, TeiidComponentException {
		if (useEntitlements && !workContext.getVDB().getDataPolicies().isEmpty()) {
			AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(workContext.getAllowedDataPolicies(), workContext.getUserName());
			visitor.setAllowCreateTemporaryTablesDefault(allowCreateTemporaryTablesByDefault);
			Request.validateWithVisitor(visitor, metadata, command);
		}		
	}
	
	@Override
	public boolean hasRole(String roleName, DQPWorkContext workContext) {
		if (!useEntitlements) {
			return true;
		}
		return workContext.getAllowedDataPolicies().containsKey(roleName);
	}

}
