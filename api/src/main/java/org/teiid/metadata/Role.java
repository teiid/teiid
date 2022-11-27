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
package org.teiid.metadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.teiid.connector.DataPlugin;
import org.teiid.metadata.Database.ResourceType;

public class Role extends AbstractMetadataRecord {

    public static class ResourceKey implements Comparable<ResourceKey> {
        private String name;
        private ResourceType type;

        @Override
        public int hashCode() {
            return Objects.hash(name==null?13:name.toLowerCase(), type);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ResourceKey)) {
                return false;
            }
            ResourceKey other = (ResourceKey)obj;
            return name.equalsIgnoreCase(other.name)
                    && Objects.equals(type, other.type);
        }

        public static ResourceKey of(String name, ResourceType type) {
            ResourceKey key = new ResourceKey();
            key.name = name;
            key.type = type==null?ResourceType.DATABASE:type;
            return key;
        }

        public String getName() {
            return name;
        }

        public ResourceType getType() {
            return type;
        }

        @Override
        public int compareTo(ResourceKey other) {
            int result = String.CASE_INSENSITIVE_ORDER.compare(name, other.name);
            if (result == 0) {
                return type.compareTo(other.type);
            }
            return result;
        }

        @Override
        public String toString() {
            return type + " " + name; //$NON-NLS-1$
        }
    }

    private static final long serialVersionUID = 1379125260214964302L;
    private List<String> mappedRoles;
    private boolean anyAuthenticated;

    private Map<ResourceKey, Permission> grants = new LinkedHashMap<>();
    private Map<ResourceKey, Map<String, Policy>> policies = new LinkedHashMap<>();

    public Role(String name) {
        super.setName(name);
    }

    /**
     * Get a copy of the mapped roles
     * @return
     */
    public List<String> getMappedRoles() {
        if (this.mappedRoles != null) {
            return new ArrayList<>(this.mappedRoles);
        }
        return mappedRoles;
    }

    public void setMappedRoles(List<String> mapped) {
        this.mappedRoles = new ArrayList<String>(mapped);
    }

    public boolean isAnyAuthenticated() {
        return this.anyAuthenticated;
    }

    public void setAnyAuthenticated(boolean b) {
        this.anyAuthenticated = b;
    }


    public Map<ResourceKey, Permission> getGrants() {
        return grants;
    }

    public Map<ResourceKey, Map<String, Policy>> getPolicies() {
        return policies;
    }

    public void addGrant(Permission grant) {
        if (grant == null) {
            return;
        }
        ResourceKey key = ResourceKey.of(grant.getResourceName(), grant.getResourceType());
        Permission previous = this.grants.get(key);
        if (previous == null) {
            this.grants.put(key, grant);
        } else {
            if (grant.getMask() != null) {
                if (previous.getMask() != null) {
                    throw new MetadataException(DataPlugin.Event.TEIID60035, DataPlugin.Util.gs(DataPlugin.Event.TEIID60035, grant.getMask(), previous.getMask()));
                }
                previous.setMask(grant.getMask());
                previous.setMaskOrder(grant.getMaskOrder());
            }
            if (grant.getCondition() != null) {
                if (previous.getCondition() != null) {
                    throw new MetadataException(DataPlugin.Event.TEIID60036, DataPlugin.Util.gs(DataPlugin.Event.TEIID60036, grant.getCondition(), previous.getCondition()));
                }
                previous.setCondition(previous.getCondition(), grant.isConditionAConstraint());
            }
            previous.appendPrivileges(grant.getPrivileges());
        }
    }

    public void removeGrant(Permission toRemoveGrant) {
        if (toRemoveGrant == null) {
            return;
        }
        ResourceKey key = ResourceKey.of(toRemoveGrant.getResourceName(), toRemoveGrant.getResourceType());
        Permission previous = this.grants.get(key);
        if (previous == null) {
            this.grants.put(key, toRemoveGrant);
        } else {
            if (toRemoveGrant.getMask() != null) {
                if (previous.getMask() != null) {
                    previous.setMask(null);
                    previous.setMaskOrder(null);
                } else {
                    //TODO: could be exception
                }
            }
            if (toRemoveGrant.getCondition() != null) {
                if (previous.getCondition() != null) {
                    previous.setCondition(null, null);
                } else {
                    //TODO: could be exception
                }
            }
            previous.removePrivileges(toRemoveGrant.getRevokePrivileges());
            if (previous.getPrivileges().isEmpty()
                    && previous.getRevokePrivileges().isEmpty()
                    && previous.getCondition() == null
                    && previous.getMask() == null) {
                this.grants.remove(key);
            }
        }
    }

    public void addPolicy(Policy policy) {
        ResourceKey key = ResourceKey.of(policy.getResourceName(), policy.getResourceType());
        Map<String, Policy> resourcePolicies = policies.computeIfAbsent(key, (k)->{return new LinkedHashMap<>();});
        if (resourcePolicies.put(policy.getName(), policy) != null) {
            throw new MetadataException(DataPlugin.Event.TEIID60041, DataPlugin.Util.gs(DataPlugin.Event.TEIID60041, policy.getName(), getName(), policy.getResourceName()));
        }
    }

    public void removePolicy(Policy policy) {
        ResourceKey key = ResourceKey.of(policy.getResourceName(), policy.getResourceType());
        Map<String, Policy> resourcePolicies = policies.get(key);
        if (resourcePolicies == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60042, DataPlugin.Util.gs(DataPlugin.Event.TEIID60042, policy.getName(), getName(), policy.getResourceName()));
        }
        if (resourcePolicies.remove(policy.getName()) == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60042, DataPlugin.Util.gs(DataPlugin.Event.TEIID60042, policy.getName(), getName(), policy.getResourceName()));
        }
    }

    public void mergeInto(Role existing) {
        for (Permission g : grants.values()) {
            existing.addGrant(g);
        }
        for (Map<String, Policy> all : policies.values()) {
            for (Policy policy : all.values()) {
                existing.addPolicy(policy);
            }
        }
    }

}
