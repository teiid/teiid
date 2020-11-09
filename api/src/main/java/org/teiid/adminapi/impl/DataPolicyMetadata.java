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
package org.teiid.adminapi.impl;

import java.io.Serializable;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.teiid.adminapi.AdminPlugin;
import org.teiid.adminapi.DataPolicy;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.metadata.Policy;


public class DataPolicyMetadata implements DataPolicy, Serializable {

    public static class ResourceKey implements Comparable<ResourceKey> {
        private String name;
        private ResourceType type;

        @Override
        public int hashCode() {
            return Objects.hash(name.toLowerCase(), type);
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

        static ResourceKey of(String name, ResourceType type) {
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

    private static final long serialVersionUID = -4119646357275977190L;

    protected String name;
    protected String description;
    protected boolean anyAuthenticated;
    protected Boolean allowCreateTemporaryTables;

    protected Map<ResourceKey, PermissionMetaData> permissions = new TreeMap<ResourceKey, PermissionMetaData>();
    protected Map<String, PermissionMetaData> languagePermissions = new HashMap<String, PermissionMetaData>(2);

    protected List<String> mappedRoleNames = new CopyOnWriteArrayList<String>();

    private Set<String> hasRowPermissions = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

    private boolean grantAll;

    private Set<String> schemas;

    private Map<org.teiid.metadata.Role.ResourceKey, Map<String, Policy>> policies;

    @Override
    public String getName() {
        return name;
    }

    public void setName(String value) {
        this.name = value;
    }

    @Override
    public String getDescription() {
        return description;
    }

    public void setDescription(String value) {
        this.description = value;
    }

    @Override
    public List<DataPermission> getPermissions() {
        List<DataPermission> result = new ArrayList<DataPermission>(this.permissions.values());
        result.addAll(this.languagePermissions.values());
        return result;
    }

    public PermissionMetaData getPermissionMetadata(String resourceName, ResourceType type) {
        PermissionMetaData result = this.permissions.get(ResourceKey.of(resourceName, type));
        if (result == null && type != ResourceType.DATABASE) {
            //legacy compatibility
            result = this.permissions.get(ResourceKey.of(resourceName, ResourceType.DATABASE));
        }
        return result;
    }

    public boolean hasRowSecurity(String resourceName) {
        return hasRowPermissions.contains(resourceName);
    }

    public void addPermission(PermissionMetaData... perms) {
        for (PermissionMetaData permission:perms) {
            addPermissionMetadata(permission);
        }
    }

    private void addPermissionMetadata(PermissionMetaData permission) {
        PermissionMetaData previous = null;
        ResourceKey key = ResourceKey.of(permission.getResourceName(), permission.getResourceType());
        if (permission.getAllowLanguage() != null) {
            previous = this.languagePermissions.put(permission.getResourceName(), permission);
        } else {
            previous = permissions.put(key, permission);
        }
        if (permission.getCondition() != null) {
            this.hasRowPermissions.add(permission.getResourceName());
        }
        if (permission.getMask() != null) {
            String resourceName = permission.getResourceName();
            int lastSegment = permission.getResourceName().lastIndexOf('.');
            if (lastSegment > 0) {
                resourceName = resourceName.substring(0, lastSegment);
            }
            this.hasRowPermissions.add(resourceName);
        }

        if (previous != null) {
            permission.bits |= previous.bits;
            permission.bitsSet |= previous.bitsSet;
            if (previous.getCondition() != null) {
                if (permission.getCondition() == null) {
                    permission.setCondition(previous.getCondition());
                    permission.setConstraint(previous.getConstraint());
                } else {
                    throw new TeiidRuntimeException(AdminPlugin.Event.TEIID70053, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70053, this.getName(), permission.getResourceName()));
                }
            }
            if (previous.getMask() != null) {
                if (permission.getMask() != null) {
                    throw new TeiidRuntimeException(AdminPlugin.Event.TEIID70053, AdminPlugin.Util.gs(AdminPlugin.Event.TEIID70053, this.getName(), permission.getResourceName()));
                }
                permission.setMask(previous.getMask());
                permission.setOrder(previous.getOrder());
            }
        }
    }

    @Override
    public List<String> getMappedRoleNames() {
        return mappedRoleNames;
    }

    public void setMappedRoleNames(List<String> names) {
        this.mappedRoleNames.clear();
        this.mappedRoleNames.addAll(names);
    }

    public void addMappedRoleName(String mappedName) {
        this.mappedRoleNames.add(mappedName);
    }

    public void removeMappedRoleName(String mappedName) {
        this.mappedRoleNames.remove(mappedName);
    }

    public Boolean allows(String resourceName, ResourceType resourceType, DataPolicy.PermissionType type) {
        PermissionMetaData p = null;
        if (type == PermissionType.LANGUAGE) {
            p = this.languagePermissions.get(resourceName);
        } else {
            p = this.permissions.get(ResourceKey.of(resourceName, resourceType));
            if (p == null && resourceType != ResourceType.DATABASE) {
                //legacy compatibility
                p = this.permissions.get(ResourceKey.of(resourceName, ResourceType.DATABASE));
            }
        }
        if (p != null) {
            return p.allows(type);
        }
        return null;
    }

    private static class RowSecurityState {
        private String condition;
        private volatile SoftReference<Object> resolvedCondition;
        private String mask;
        private volatile SoftReference<Object> resolvedMask;
        private Integer order;
        private Boolean constraint;
    }

    public static class PermissionMetaData implements DataPermission, Serializable {
        private static final long serialVersionUID = 7034744531663164277L;

        // XML based fields
        private String resourceName;
        protected byte bits;
        protected byte bitsSet;
        private ResourceType resourceType;

        private RowSecurityState rowSecurityState;

        @Override
        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String value) {
            this.resourceName = value;
        }

        public void setResourceType(ResourceType resourceType) {
            this.resourceType = resourceType;
        }

        @Override
        public ResourceType getResourceType() {
            return resourceType;
        }

        @Override
        public Boolean getAllowCreate() {
            return bitSet(0x01);
        }

        private Boolean bitSet(int bitMask) {
            if ((bitsSet & bitMask) == bitMask) {
                if ((bits & bitMask) == bitMask) {
                    return Boolean.TRUE;
                }
                return Boolean.FALSE;
            }
            return null;
        }

        private void setBit(int bitMask, Boolean bool) {
            if (bool == null) {
                bitsSet &= (~bitMask);
                bits &= (~bitMask);
                return;
            }
            bitsSet |= bitMask;
            if (bool) {
                bits |= bitMask;
            } else {
                bits &= (~bitMask);
            }
        }

        public void setAllowCreate(Boolean value) {
            setBit(0x01, value);
        }

        @Override
        public Boolean getAllowRead() {
            return bitSet(0x02);
        }

        public void setAllowRead(Boolean value) {
            setBit(0x02, value);
        }

        @Override
        public Boolean getAllowUpdate() {
            return bitSet(0x04);
        }

        public void setAllowUpdate(Boolean value) {
            setBit(0x04, value);
        }

        @Override
        public Boolean getAllowDelete() {
            return bitSet(0x08);
        }

        public void setAllowDelete(Boolean value) {
            setBit(0x08, value);
        }

        public String getType() {
            StringBuilder sb = new StringBuilder();
            if (Boolean.TRUE.equals(getAllowCreate())) {
                sb.append("C");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowRead())) {
                sb.append("R");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowUpdate())) {
                sb.append("U");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowDelete())) {
                sb.append("D");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowExecute())) {
                sb.append("E");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowAlter())) {
                sb.append("A");//$NON-NLS-1$
            }
            if (Boolean.TRUE.equals(getAllowLanguage())) {
                sb.append("L");//$NON-NLS-1$
            }
            if (rowSecurityState != null) {
                if (rowSecurityState.condition != null) {
                    sb.append(" condition ").append(rowSecurityState.condition); //$NON-NLS-1$
                }
                if (rowSecurityState.mask != null) {
                    sb.append(" mask ").append(rowSecurityState.mask); //$NON-NLS-1$
                    if (rowSecurityState.order != null) {
                        sb.append(" order ").append(rowSecurityState.order); //$NON-NLS-1$
                    }
                }
                if (rowSecurityState.constraint != null) {
                    sb.append(" constraint ").append(rowSecurityState.constraint); //$NON-NLS-1$
                }
            }
            return sb.toString();
        }

        public PermissionMetaData clone() {
            PermissionMetaData clone = new PermissionMetaData();
            clone.bits = bits;
            clone.bitsSet = bitsSet;
            clone.resourceName = resourceName;
            clone.resourceType = resourceType;
            if (rowSecurityState != null) {
                clone.rowSecurityState = new RowSecurityState();
                clone.rowSecurityState.condition = rowSecurityState.condition;
                clone.rowSecurityState.mask = rowSecurityState.mask;
                clone.rowSecurityState.constraint = rowSecurityState.constraint;
                clone.rowSecurityState.order = rowSecurityState.order;
            }
            return clone;
        }

        public Boolean allows(PermissionType type) {
            switch (type) {
            case ALTER:
                return getAllowAlter();
            case CREATE:
                return getAllowCreate();
            case EXECUTE:
                if (getAllowExecute() != null) {
                    return getAllowExecute();
                }
            case READ:
                return getAllowRead();
            case UPDATE:
                return getAllowUpdate();
            case DELETE:
                return getAllowDelete();
            case LANGUAGE:
                return getAllowLanguage();
            case DROP:
                //not implemented
            default:
                break;
            }
            throw new AssertionError();
        }

        @Override
        public Boolean getAllowAlter() {
            return bitSet(0x10);
        }

        @Override
        public Boolean getAllowExecute() {
            return bitSet(0x20);
        }

        public void setAllowAlter(Boolean allowAlter) {
            setBit(0x10, allowAlter);
        }

        public void setAllowExecute(Boolean allowExecute) {
            setBit(0x20, allowExecute);
        }

        @Override
        public Boolean getAllowLanguage() {
            return bitSet(0x40);
        }

        public void setAllowLanguage(Boolean value) {
            setBit(0x40, value);
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getResourceName());
            sb.append("["); //$NON-NLS-1$
            sb.append(getType());
            sb.append("]");//$NON-NLS-1$
            return sb.toString();
        }

        @Override
        public String getCondition() {
            if (rowSecurityState == null) {
                return null;
            }
            return rowSecurityState.condition;
        }

        public void setCondition(String filter) {
            if (rowSecurityState == null) {
                if (filter == null) {
                    return;
                }
                rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.condition = filter;
        }

        @Override
        public String getMask() {
            if (rowSecurityState == null) {
                return null;
            }
            return rowSecurityState.mask;
        }

        public void setMask(String mask) {
            if (rowSecurityState == null) {
                if (mask == null) {
                    return;
                }
                rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.mask = mask;
        }

        @Override
        public Integer getOrder() {
            if (rowSecurityState == null) {
                return null;
            }
            return rowSecurityState.order;
        }

        public void setOrder(Integer order) {
            if (rowSecurityState == null) {
                if (order == null) {
                    return;
                }
                rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.order = order;
        }

        public Object getResolvedCondition() {
            if (rowSecurityState == null) {
                return null;
            }
            if (rowSecurityState.resolvedCondition != null) {
                return rowSecurityState.resolvedCondition.get();
            }
            return null;
        }

        public void setResolvedCondition(Object resolvedCondition) {
            if (rowSecurityState == null) {
                rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.resolvedCondition = new SoftReference<Object>(resolvedCondition);
        }

        public Object getResolvedMask() {
            if (rowSecurityState == null) {
                return null;
            }
            if (rowSecurityState.resolvedMask != null) {
                return rowSecurityState.resolvedMask.get();
            }
            return null;
        }

        public void setResolvedMask(Object resolvedMask) {
            if (rowSecurityState == null) {
                rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.resolvedMask = new SoftReference<Object>(resolvedMask);
        }

        @Override
        public Boolean getConstraint() {
            if (rowSecurityState == null) {
                return null;
            }
            return rowSecurityState.constraint;
        }

        public void setConstraint(Boolean constraint) {
            if (rowSecurityState == null) {
                if (constraint == null) {
                    return;
                }
                this.rowSecurityState = new RowSecurityState();
            }
            this.rowSecurityState.constraint = constraint;
        }

    }

    public Boolean isAllowCreateTemporaryTables() {
        return allowCreateTemporaryTables;
    }

    public void setAllowCreateTemporaryTables(Boolean allowCreateTemporaryTables) {
        this.allowCreateTemporaryTables = allowCreateTemporaryTables;
    }

    @Override
    public boolean isAnyAuthenticated() {
        return this.anyAuthenticated;
    }

    public void setAnyAuthenticated(boolean anyAuthenticated) {
        this.anyAuthenticated = anyAuthenticated;
    }

    @Override
    public boolean isGrantAll() {
        return this.grantAll;
    }

    public void setGrantAll(boolean grantAll) {
        this.grantAll = grantAll;
    }

    public DataPolicyMetadata clone() {
        DataPolicyMetadata clone = new DataPolicyMetadata();
        clone.allowCreateTemporaryTables = this.allowCreateTemporaryTables;
        clone.anyAuthenticated = this.anyAuthenticated;
        clone.description = this.description;
        clone.hasRowPermissions = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
        clone.hasRowPermissions.addAll(this.hasRowPermissions);
        clone.languagePermissions = new HashMap<String, DataPolicyMetadata.PermissionMetaData>(this.languagePermissions);
        clone.mappedRoleNames = this.mappedRoleNames; //direct reference to preserve updates
        clone.name = this.name;
        clone.grantAll = this.grantAll;
        clone.permissions = new TreeMap<>();
        //create a full copy to prevent
        for (PermissionMetaData perm : permissions.values()) {
            clone.addPermissionMetadata(perm.clone());
        }
        if (policies != null) {
            clone.policies = new LinkedHashMap<org.teiid.metadata.Role.ResourceKey, Map<String, Policy>>(policies);
        }
        return clone;
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public void setSchemas(Set<String> schemas) {
        this.schemas = schemas;
    }

    public void addPolicies(
            Map<org.teiid.metadata.Role.ResourceKey, Map<String, Policy>> policies) {
        policies.keySet().stream().forEach((k)->hasRowPermissions.add(k.getName()));
        if (this.policies == null) {
            this.policies = new LinkedHashMap<>();
        }
        this.policies.putAll(policies);
    }

    public Map<org.teiid.metadata.Role.ResourceKey, Map<String, Policy>> getPolicies() {
        return policies;
    }

    public Map<String, Policy> getPolicies(org.teiid.metadata.Database.ResourceType type, String name) {
        if (this.policies == null) {
            return null;
        }
        return policies.get(org.teiid.metadata.Role.ResourceKey.of(name, type));
    }

}
