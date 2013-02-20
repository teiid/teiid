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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;

public class DataRolePolicyDecider implements PolicyDecider {

    private boolean allowCreateTemporaryTablesByDefault = false;
    private boolean allowFunctionCallsByDefault = false;

	@Override
	public Set<String> getInaccessibleResources(PermissionType action,
			Set<String> resources, Context context, CommandContext commandContext) {
		if (action == PermissionType.EXECUTE && context == Context.FUNCTION && allowFunctionCallsByDefault) {
			return Collections.emptySet();
		}
		List<DataPolicy> policies = new ArrayList<DataPolicy>(commandContext.getAllowedDataPolicies().values());
		int policyCount = policies.size();
		outer:for (Iterator<String> iter = resources.iterator(); iter.hasNext();) {
			String resource = iter.next();
			while (resource.length() > 0) {
				boolean isFalse = false;
				for (int j = 0; j < policyCount; j++) {
					DataPolicyMetadata policy = (DataPolicyMetadata)policies.get(j);
					Boolean allows = policy.allows(resource, action);
					if (allows != null) {
						if (allows) {
							iter.remove();
							continue outer;
						}
						isFalse = true;
					}
				}
				if (isFalse || action == PermissionType.LANGUAGE) {
					break; //don't check less specific permissions
				}
				resource = resource.substring(0, Math.max(0, resource.lastIndexOf('.')));
			}
		}
		return resources;
	}

	@Override
	public boolean hasRole(String roleName, CommandContext context) {
		return context.getAllowedDataPolicies().containsKey(roleName);
	}

	@Override
	public boolean isTempAccessable(PermissionType action, String resource,
			Context context, CommandContext commandContext) {
		if (resource != null) {
			return getInaccessibleResources(action, new HashSet<String>(Arrays.asList(resource)), context, commandContext).isEmpty();
		}
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
