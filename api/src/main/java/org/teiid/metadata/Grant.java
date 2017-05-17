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
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

import org.teiid.metadata.Database.ResourceType;

public class Grant extends AbstractMetadataRecord {
    private static final long serialVersionUID = 3728259393244582775L;

    public static class Permission {
        public enum Privilege {
            SELECT, INSERT, UPDATE, DELETE, EXECUTE,
            ALTER, DROP,
            USAGE, 
            ALL_PRIVILEGES("ALL PRIVILEGES"), //$NON-NLS-1$ 
            TEMPORARY_TABLE("TEMPORARY TABLE"), //$NON-NLS-1$ 
            CREATE;
            
            private final String toString;
            
            Privilege(String toString) {
                this.toString = toString;
            }
            
            Privilege() {
                this.toString = name();
            }
            
            public String toString() {
                return toString;
            }
        }        
        private Database.ResourceType resourceType= null;
        private String resource = null;
        private String mask = null;
        private Integer maskOrder;
        private String condition = null;
        private Boolean isConstraint;
        private EnumSet<Privilege> privileges = EnumSet.noneOf(Privilege.class);
        private EnumSet<Privilege> revokePrivileges = EnumSet.noneOf(Privilege.class);
        
        public Database.ResourceType getResourceType() {
            if (resourceType == null) {
                return ResourceType.DATABASE;
            }
            return resourceType;
        }
        
        public void setResourceType(Database.ResourceType on) {
            this.resourceType = on;
        }
        
        public String getResourceName() {
            return resource;
        }
        
        public void setResourceName(String resource) {
            this.resource = resource;
        }
        
        public String getMask() {
            return mask;
        }
        
        public void setMask(String mask) {
            this.mask = mask;
        }
        
        public Integer getMaskOrder() {
            return maskOrder;
        }
        
        public void setMaskOrder(Integer maskOrder) {
            this.maskOrder = maskOrder;
        }
        
        public String getCondition() {
            return condition;
        }
        
        public void setCondition(String condition, Boolean isConstraint) {
            this.condition = condition;
            this.isConstraint = isConstraint;
        }
        
        public Boolean isConditionAConstraint() {
            return isConstraint;
        }

        public EnumSet<Privilege> getPrivileges() {
            return privileges;
        }
        
        public EnumSet<Privilege> getRevokePrivileges() {
            return revokePrivileges;
        }
        
        public Boolean hasPrivilege(Privilege allow) {
            if (this.privileges.contains(allow)) {
                return true;
            }
            if (this.revokePrivileges.contains(allow)) {
                return false;
            }
            return null;
        }
        
        public void setPrivileges(List<Privilege> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            this.privileges = EnumSet.copyOf(types);
        }
        
        public void setRevokePrivileges(List<Privilege> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            this.revokePrivileges = EnumSet.copyOf(types);
        }

        public void appendPrivileges(EnumSet<Privilege> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            for (Privilege a:types) {
                this.privileges.add(a);
                this.revokePrivileges.remove(a);
            }
        }
        
        public void removePrivileges(EnumSet<Privilege> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            for (Privilege a:types) {
                if (!this.privileges.remove(a)) {
                    this.revokePrivileges.add(a);
                }
            }
        }
        
        private void setAllows(Boolean allow, Privilege privilege) {
            if(allow!= null) {
                if (allow) {
                    this.revokePrivileges.remove(privilege);
                    this.privileges.add(privilege);
                } else {
                    if (!this.privileges.remove(privilege)) {
                        this.revokePrivileges.add(privilege);
                    }
                }
            }
        }

        public void setAllowSelect(Boolean allow) {
            setAllows(allow, Privilege.SELECT);
        }
        
        public void setAllowAlter(Boolean allow) {
            setAllows(allow, Privilege.ALTER);
        }

        public void setAllowInsert(Boolean allow) {
            setAllows(allow, Privilege.INSERT);
        }

        public void setAllowDelete(Boolean allow) {
            setAllows(allow, Privilege.DELETE);
        }

        public void setAllowExecute(Boolean allow) {
            setAllows(allow, Privilege.EXECUTE);
        }

        public void setAllowUpdate(Boolean allow) {
            setAllows(allow, Privilege.UPDATE);
        }
        
        public void setAllowDrop(Boolean allow) {
            setAllows(allow, Privilege.DROP);
        }        

        public void setAllowUsage(Boolean allow) {
            setAllows(allow, Privilege.USAGE);
        }        
        public void setAllowAllPrivileges(Boolean allow) {
            setAllows(allow, Privilege.ALL_PRIVILEGES);
        }
        public void setAllowTemporyTables(Boolean allow) {
            setAllows(allow, Privilege.TEMPORARY_TABLE);
        }

        public boolean resourceMatches(Permission other) {
            if (getResourceType() != other.getResourceType()) {
                return false;
            }
            if (resource == null && other.resource == null) {
                return true;
            }
            if (resource != null && other.resource != null && resource.equalsIgnoreCase(other.resource)) {
                return true;
            }
            return false;
        }        
    }
    
    protected List<Permission> permissions = new ArrayList<Permission>();
    private String role;
    
    public Collection<Grant.Permission> getPermissions() {
        return this.permissions;
    }
    
    public void addPermission(Grant.Permission permission) {
        this.permissions.add(permission);
    }

    void removePermission(Grant.Permission permission) {
        this.permissions.remove(permission);
    }
    
    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }
}
