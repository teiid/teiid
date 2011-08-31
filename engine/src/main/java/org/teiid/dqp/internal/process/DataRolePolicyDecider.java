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

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;

public class DataRolePolicyDecider implements PolicyDecider {

    private boolean allowCreateTemporaryTablesByDefault = true;
    private boolean allowFunctionCallsByDefault = true;

	@Override
	public Set<String> getInaccessibleResources(PermissionType action,
			Set<String> resources, Context context, CommandContext commandContext) {
		if (action == PermissionType.EXECUTE && context == Context.FUNCTION && allowFunctionCallsByDefault) {
			return Collections.emptySet();
		}
		LinkedHashSet<String> results = new LinkedHashSet<String>(resources);
		for(DataPolicy p:commandContext.getAllowedDataPolicies().values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			
			if (results.isEmpty()) {
				break;
			}
			
			Iterator<String> i = results.iterator();
			while (i.hasNext()) {				
				if (policy.allows(i.next(), action)) {
					i.remove();
				}
			}
		}
		return results;
	}

	@Override
	public boolean hasRole(String roleName, CommandContext context) {
		return context.getAllowedDataPolicies().containsKey(roleName);
	}

	@Override
	public boolean isTempAccessable(PermissionType action, String resource,
			Context context, CommandContext commandContext) {
		Boolean result = null;
    	for(DataPolicy p:commandContext.getAllowedDataPolicies().values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			
			if (policy.isAllowCreateTemporaryTables() != null) {
				if (policy.isAllowCreateTemporaryTables()) {
					return true;
				}
				result = policy.isAllowCreateTemporaryTables();
			}
		}
    	if (result != null) {
    		return result;
    	}
    	return allowCreateTemporaryTablesByDefault;
	}
	
    public void setAllowCreateTemporaryTablesByDefault(
			boolean allowCreateTemporaryTablesByDefault) {
		this.allowCreateTemporaryTablesByDefault = allowCreateTemporaryTablesByDefault;
	}
    
    public void setAllowFunctionCallsByDefault(boolean allowFunctionCallsDefault) {
		this.allowFunctionCallsByDefault = allowFunctionCallsDefault;
	}
    
    @Override
    public boolean validateCommand(CommandContext commandContext) {
    	return !commandContext.getVdb().getDataPolicies().isEmpty();
    }

}
