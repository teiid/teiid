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
import java.util.List;
import java.util.Set;

import org.teiid.CommandContext;
import org.teiid.PolicyDecider;
import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.Context;
import org.teiid.adminapi.DataPolicy.PermissionType;
import org.teiid.adminapi.DataPolicy.ResourceType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.core.util.Assertion;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.metadata.AbstractMetadataRecord;
import org.teiid.metadata.Column;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Table;

public class DataRolePolicyDecider implements PolicyDecider {

    private boolean allowCreateTemporaryTablesByDefault = PropertiesUtils.getHierarchicalProperty("org.teiid.allowCreateTemporaryTablesByDefault", false, Boolean.class); //$NON-NLS-1$
    private boolean allowFunctionCallsByDefault = PropertiesUtils.getHierarchicalProperty("org.teiid.allowFunctionCallsByDefault", false, Boolean.class); //$NON-NLS-1$

    @Override
    public Set<AbstractMetadataRecord> getInaccessibleResources(PermissionType action,
            Set<AbstractMetadataRecord> resources, Context context, CommandContext commandContext) {
        if ((action == PermissionType.EXECUTE || action == null) && context == Context.FUNCTION && allowFunctionCallsByDefault) {
            return Collections.emptySet();
        }
        Collection<DataPolicy> policies = commandContext.getAllowedDataPolicies().values();
        int policyCount = policies.size();
        boolean[] exclude = new boolean[policyCount];
        Boolean[] results = null;
        List<PermissionType> metadataPermissions = null;
        if (context == Context.METADATA) {
            Assertion.assertTrue(resources.size() == 1);
            if (action == PermissionType.READ) {
                results = new Boolean[5];
                metadataPermissions = Arrays.asList(
                        PermissionType.ALTER, PermissionType.CREATE,
                        PermissionType.UPDATE, PermissionType.READ,
                        PermissionType.DELETE);
            } else {
                results = new Boolean[2];
                metadataPermissions = Arrays.asList(PermissionType.ALTER, action);
            }
        }
        outer:for (Iterator<AbstractMetadataRecord> iter = resources.iterator(); iter.hasNext();) {
            AbstractMetadataRecord resource = iter.next();
            Arrays.fill(exclude, false);
            while (resource != null) {
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
                        if (resource instanceof Schema
                                && policy.getSchemas().contains(resource.getName())) {
                            iter.remove();
                            continue outer;
                        }
                        continue;
                    }
                    if (context == Context.METADATA) {
                        for (int i = 0; i < results.length; i++) {
                            Boolean allows = policy.allows(resource.getFullName(), getResourceType(resource), metadataPermissions.get(i));
                            if (allows != null && allows && results[i] == null) {
                                resources.clear();
                                return resources;
                            }
                            if (results[i] == null) {
                                results[i] = allows;
                            }
                        }
                    }
                    Boolean allows = policy.allows(resource.getFullName(), getResourceType(resource), action);
                    if (allows != null) {
                        if (allows) {
                            iter.remove();
                            continue outer;
                        }
                        exclude[j] = true;
                    }
                }
                resource = resource.getParent();
            }
        }
        return resources;
    }

    @Override
    public boolean isLanguageAllowed(String language,
            CommandContext commandContext) {
        Collection<DataPolicy> policies = commandContext.getAllowedDataPolicies().values();
        for (DataPolicy policy : policies) {
            if (policy.isGrantAll()) {
                return true;
            }
            Boolean allows = ((DataPolicyMetadata)policy).allows(language, ResourceType.LANGUAGE, PermissionType.LANGUAGE);
            if (allows != null && allows) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean hasRole(String roleName, CommandContext context) {
        return context.getAllowedDataPolicies().containsKey(roleName);
    }

    @Override
    public boolean isTempAccessible(PermissionType action, AbstractMetadataRecord resource,
            Context context, CommandContext commandContext) {
        if (resource != null) {
            return getInaccessibleResources(action, new HashSet<AbstractMetadataRecord>(Arrays.asList(resource)), context, commandContext).isEmpty();
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

    //we're doing this here rather than on the metadata record to
    //avoid confusion with Database.ResourceType
    //TODO: fix that - maybe combine admin and api
    public ResourceType getResourceType(AbstractMetadataRecord record) {
        if (record instanceof Table) {
            return ResourceType.TABLE;
        }
        if (record instanceof Procedure) {
            Procedure p = (Procedure)record;
            if (p.isFunction()) {
                return ResourceType.FUNCTION;
            }
            return ResourceType.PROCEDURE;
        }
        if (record instanceof Column) {
            return ResourceType.COLUMN;
        }
        if (record instanceof FunctionMethod) {
            return ResourceType.FUNCTION;
        }
        if (record instanceof Schema) {
            return ResourceType.SCHEMA;
        }
        //effectively the default, which means anything
        return ResourceType.DATABASE;
    }

}
