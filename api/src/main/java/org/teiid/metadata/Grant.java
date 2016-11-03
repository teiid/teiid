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
package org.teiid.metadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;

public class Grant extends AbstractMetadataRecord {
    private static final long serialVersionUID = 3728259393244582775L;

    public static class Permission {
        public enum Allowance {
            SELECT, INSERT, UPDATE, DELETE, EXECUTE, LANGUAGE, ALTER, DROP, ALL_PRIVILEGES, TEMPORARY_TABLE, CREATE
        }        
        private Database.ResourceType resourceType= null;
        private String resource = null;
        private String mask = null;
        private Integer maskOrder;
        private String condition = null;
        private Boolean isConstraint;
        private EnumSet<Allowance> allowances = EnumSet.noneOf(Allowance.class);
        
        public Database.ResourceType getResourceType() {
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

        public EnumSet<Allowance> getAllowances() {
            return allowances;
        }
        
        public boolean hasAllowance(Allowance allow) {
            return this.allowances.contains(allow);
        }
        
        public void setAllowances(List<Allowance> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            this.allowances = EnumSet.copyOf(types);
        }

        public void appendAllowances(EnumSet<Allowance> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            for (Allowance a:types) {
                this.allowances.add(a);
            }
        }
        
        public void removeAllowances(EnumSet<Allowance> types) {
            if (types == null ||types.isEmpty()) {
                return;
            }
            for (Allowance a:types) {
                this.allowances.remove(a);
            }
        }

        public void setAllowSelect(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.SELECT);
            }
        }
        
        public void setAllowAlter(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.ALTER);
            }
        }

        public void setAllowInsert(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.INSERT);
            }
        }

        public void setAllowDelete(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.DELETE);
            }
        }

        public void setAllowExecute(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.EXECUTE);
            }
        }

        public void setAllowUpdate(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.UPDATE);
            }
        }
        
        public void setAllowDrop(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.DROP);
            }
        }        

        public void setAllowLanguage(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.LANGUAGE);
            }
        }        
        public void setAllowAllPrivileges(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.ALL_PRIVILEGES);
            }
        }
        public void setAllowTemporyTables(Boolean allow) {
            if(allow!= null && allow) {
                this.allowances.add(Allowance.TEMPORARY_TABLE);
            }
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
