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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.core.util.PropertiesUtils;

public class DataRolePolicyDecider implements PolicyDecider {

    private boolean allowCreateTemporaryTablesByDefault = PropertiesUtils.getHierarchicalProperty("org.teiid.allowCreateTemporaryTablesByDefault", false, Boolean.class); //$NON-NLS-1$
    private boolean allowFunctionCallsByDefault = PropertiesUtils.getHierarchicalProperty("org.teiid.allowFunctionCallsByDefault", false, Boolean.class); //$NON-NLS-1$

	@Override
	public Set<String> getInaccessibleResources(PermissionType action,
			Set<String> resources, Context context, CommandContext commandContext) {
		if (action == PermissionType.EXECUTE && context == Context.FUNCTION && allowFunctionCallsByDefault) {
			return Collections.emptySet();
		}
		Collection<DataPolicy> policies = commandContext.getAllowedDataPolicies().values();
		int policyCount = policies.size();
		boolean[] exclude = new boolean[policyCount];
		outer:for (Iterator<String> iter = resources.iterator(); iter.hasNext();) {
			String resource = iter.next();
			Arrays.fill(exclude, false);
			int excludeCount = 0;
			while (resource.length() > 0) {
				Iterator<DataPolicy> policyIter = policies.iterator();
				for (int j = 0; j < policyCount; j++) {
					DataPolicyMetadata policy = (DataPolicyMetadata)policyIter.next();
					if (exclude[j]) {
						continue;
					}
					if (policy.isGrantAll()) {
						if (policy.getSchemas() == null) {
							resources.clear();
							return resources;
						}
						if (action == PermissionType.LANGUAGE) {
							iter.remove();
							continue outer;
						}
						//imported grant all must be checked against the schemas
						if (resource.indexOf('.') > 0) {
							continue;
						}
						if (policy.getSchemas().contains(resource)) {
							iter.remove();
							continue outer;
						}
						continue;
					}
					Boolean allows = policy.allows(resource, action);
					if (allows != null) {
						if (allows) {
							iter.remove();
							continue outer;
						}
						exclude[j] = true;
						excludeCount++;
					}
				}
				if (excludeCount == policyCount || action == PermissionType.LANGUAGE) {
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
	public boolean isTempAccessible(PermissionType action, String resource,
			Context context, CommandContext commandContext) {
		if (resource != null) {
			return getInaccessibleResources(action, new HashSet<String>(Arrays.asList(resource)), context, commandContext).isEmpty();
		}
		Boolean result = null;
    	for(DataPolicy p:commandContext.getAllowedDataPolicies().values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			if (policy.isGrantAll()) {
				return true;
			}
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
