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

import org.teiid.PolicyDecider;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.core.TeiidComponentException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.util.CommandContext;

/**
 * The default Teiid authorization validator
 */
public class DefaultAuthorizationValidator implements AuthorizationValidator {
	
	private boolean enabled = true;
	private PolicyDecider policyDecider;
	
	public DefaultAuthorizationValidator() {
	}

	@Override
	public boolean validate(String[] originalSql, Command command,
			QueryMetadataInterface metadata, CommandContext commandContext,
			CommandType commandType) throws QueryValidatorException,
			TeiidComponentException {
		if (enabled && policyDecider.validateCommand(commandContext)) {
			AuthorizationValidationVisitor visitor = new AuthorizationValidationVisitor(this.policyDecider, commandContext);
			Request.validateWithVisitor(visitor, metadata, command);
		}		
		return false;
	}
	
	@Override
	public boolean hasRole(String roleName, CommandContext commandContext) {
		if (!enabled) {
			return true;
		}
		return this.policyDecider.hasRole(roleName, commandContext);
	}
	
	public void setPolicyDecider(PolicyDecider policyDecider) {
		this.policyDecider = policyDecider;
	}
	
	public PolicyDecider getPolicyDecider() {
		return policyDecider;
	}
	
	@Override
	public boolean isEnabled() {
		return enabled;
	}
	
	@Override
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}
	
}
