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
package org.teiid.adminapi.impl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlType;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.teiid.adminapi.DataRole;


@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "", propOrder = {
    "description",
    "permissions",
    "mappedRoleNames"
})
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class DataRoleMetadata implements DataRole, Serializable {
	private static final long serialVersionUID = -4119646357275977190L;
	
	@XmlAttribute(name = "name", required = true)
    protected String name;
	@XmlElement(name = "description")
    protected String description;

    @XmlElement(name = "permission")
    protected ListOverMap<PermissionMetaData> permissions = new ListOverMap<PermissionMetaData>(new KeyBuilder<PermissionMetaData>() {
		@Override
		public String getKey(PermissionMetaData entry) {
			return entry.getResourceName();
		}
	});	
    
    @XmlElement(name = "mapped-role-name")
    protected List<String> mappedRoleNames;

	@Override
	@ManagementProperty(description="Role Name")
	@ManagementObjectID(type="role")
    public String getName() {
        return name;
    }

    public void setName(String value) {
        this.name = value;
    }
    
    @Override
    @ManagementProperty(description="Role Description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String value) {
        this.description = value;
    }

	@Override
	@ManagementProperty(description="Permissions in a Data Role", managed=true)
	public List<Permission> getPermissions() {
		return new ArrayList<Permission>(this.permissions.getMap().values());
	}
	
	public void setPermissions(List<Permission> permissions) {
		this.permissions.getMap().clear();
		for (Permission permission:permissions) {
			this.permissions.getMap().put(permission.getResourceName(), (PermissionMetaData)permission);
		}
	}	
	
	public PermissionMetaData getPermission(String resourceName) {
		return this.permissions.getMap().get(resourceName);
	}
	
	public void addPermission(PermissionMetaData permission) {
		this.permissions.getMap().put(permission.getResourceName(), permission);
	}
	
    @Override
    @ManagementProperty(description="Mapped Container role names mapped to this role")    
    public List<String> getMappedRoleNames() {
		return mappedRoleNames;
	}

	public void setMappedRoleNames(List<String> names) {
		this.mappedRoleNames = names;
	}    
	
	
    @XmlAccessorType(XmlAccessType.FIELD)
    @XmlType(name = "", propOrder = {
        "resourceName",
        "allowCreate",
        "allowRead",
        "allowUpdate",
        "allowDelete"
    })	
    @ManagementObject(properties=ManagementProperties.EXPLICIT)
	public static class PermissionMetaData implements Permission{
        @XmlElement(name = "resource-name", required = true)
        protected String resourceName;
        @XmlElement(name = "allow-create")
        protected Boolean allowCreate;
        @XmlElement(name = "allow-read")
        protected Boolean allowRead;
        @XmlElement(name = "allow-update")
        protected Boolean allowUpdate;
        @XmlElement(name = "allow-delete")
        protected Boolean allowDelete;
        
        @Override
        @ManagementProperty(description="Resource Name, for which role defined")
        @ManagementObjectID(type="permission")
        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String value) {
            this.resourceName = value;
        }

        @Override
        @ManagementProperty(description="Allows Create")
        public boolean isAllowCreate() {
        	if (allowCreate == null) {
        		return false;
        	}
            return allowCreate;
        }

        public void setAllowCreate(Boolean value) {
            this.allowCreate = value;
        }

        @Override
        @ManagementProperty(description="Allows Read")
        public boolean isAllowRead() {
        	if (allowRead == null) {
        		return false;
        	}
            return allowRead;
        }

        public void setAllowRead(Boolean value) {
            this.allowRead = value;
        }

        @Override
        @ManagementProperty(description="Allows Update")
        public boolean isAllowUpdate() {
        	if (allowUpdate == null) {
        		return false;
        	}
            return allowUpdate;
        }

        public void setAllowUpdate(Boolean value) {
            this.allowUpdate = value;
        }

        @Override
        @ManagementProperty(description="Allows Delete")
        public boolean isAllowDelete() {
        	if (allowDelete == null) {
        		return false;
        	}
            return allowDelete;
        }

        public void setAllowDelete(Boolean value) {
            this.allowDelete = value;
        }        
	}
}
