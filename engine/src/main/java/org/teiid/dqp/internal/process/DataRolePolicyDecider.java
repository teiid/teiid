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
    	for(DataPolicy p:commandContext.getAllowedDataPolicies().values()) {
			DataPolicyMetadata policy = (DataPolicyMetadata)p;
			
			if (policy.isAllowCreateTemporaryTables() != null) {
				return policy.isAllowCreateTemporaryTables();
			}
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
