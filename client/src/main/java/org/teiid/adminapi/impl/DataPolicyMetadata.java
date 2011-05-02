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
import org.teiid.adminapi.DataPolicy;


@XmlAccessorType(XmlAccessType.NONE)
@XmlType(name = "", propOrder = {
    "description",
    "permissions",
    "mappedRoleNames"
})
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class DataPolicyMetadata implements DataPolicy, Serializable {
	private static final long serialVersionUID = -4119646357275977190L;
	
	@XmlAttribute(name = "name", required = true)
    protected String name;
	@XmlElement(name = "description")
    protected String description;
	@XmlAttribute(name = "any-authenticated")
	protected boolean anyAuthenticated;
	@XmlAttribute(name = "allow-create-temporary-tables")
	protected Boolean allowCreateTemporaryTables;

    @XmlElement(name = "permission")
    protected PermissionMap permissions = new PermissionMap(new KeyBuilder<PermissionMetaData>() {
		private static final long serialVersionUID = -6992984146431492449L;
		@Override
		public String getKey(PermissionMetaData entry) {
			return entry.getResourceName().toLowerCase();
		}
	});
    
    @XmlElement(name = "mapped-role-name")
    protected List<String> mappedRoleNames = new ArrayList<String>();

	@Override
	@ManagementProperty(description="Policy Name")
	@ManagementObjectID(type="policy")
    public String getName() {
        return name;
    }

    public void setName(String value) {
        this.name = value;
    }
    
    @Override
    @ManagementProperty(description="Policy Description")
    public String getDescription() {
        return description;
    }

    public void setDescription(String value) {
        this.description = value;
    }

	@Override
	@ManagementProperty(description="Permissions in a Data Policy", managed=true)
	public List<DataPermission> getPermissions() {
		return new ArrayList<DataPermission>(this.permissions.getMap().values());
	}
	
	public void setPermissions(List<DataPermission> permissions) {
		this.permissions.getMap().clear();
		for (DataPermission permission:permissions) {
			this.permissions.add((PermissionMetaData)permission);
		}
	}	
	
	public void addPermission(PermissionMetaData... permissions) {
		for (PermissionMetaData permission:permissions) {
			this.permissions.add(permission);
		}
	}
	
    @Override
    @ManagementProperty(description="Mapped Container role names mapped to this policy")    
    public List<String> getMappedRoleNames() {
		return mappedRoleNames;
	}

	public void setMappedRoleNames(List<String> names) {
		this.mappedRoleNames.clear();
		this.mappedRoleNames.addAll(names);
	}    
	
	public void addMappedRoleName(String name) {
		this.mappedRoleNames.add(name);
	}  	
	
	public boolean allows(String resourceName, DataPolicy.PermissionType type) {
		resourceName = resourceName.toLowerCase();
		while (resourceName.length() > 0) {
			PermissionMetaData p = this.permissions.getMap().get(resourceName);
			if (p != null) {
				Boolean allowed = p.allows(type);
				if (allowed != null) {
					return allowed;
				}
			}
			resourceName = resourceName.substring(0, Math.max(0, resourceName.lastIndexOf('.')));
		}
		return false;
	}
	
	
    @XmlAccessorType(XmlAccessType.NONE)
    @XmlType(name = "", propOrder = {
        "resourceName",
        "allowCreate",
        "allowRead",
        "allowUpdate",
        "allowDelete",
        "allowExecute",
        "allowAlter"
    })	
    @ManagementObject(properties=ManagementProperties.EXPLICIT)
	public static class PermissionMetaData implements DataPermission, Serializable {
		private static final long serialVersionUID = 7034744531663164277L;
        
        // XML based fields
        private String resourceName;
        @XmlElement(name = "allow-create")
        protected Boolean allowCreate;
        @XmlElement(name = "allow-read")
        protected Boolean allowRead;
        @XmlElement(name = "allow-update")
        protected Boolean allowUpdate;
        @XmlElement(name = "allow-delete")
        protected Boolean allowDelete;
        @XmlElement(name = "allow-execute")
        protected Boolean allowExecute;
        @XmlElement(name = "allow-alter")
        protected Boolean allowAlter;
        
        @Override
        @ManagementProperty(description="Resource Name, for which permission defined")
        //@ManagementObjectID(type="permission")
        @XmlElement(name = "resource-name", required = true)
        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String value) {
            this.resourceName = value;
        }

        @Override
        @ManagementProperty(description="Allows Create")
        public Boolean getAllowCreate() {
            return allowCreate;
        }

        public void setAllowCreate(Boolean value) {
            this.allowCreate = value;
        }

        @Override
        @ManagementProperty(description="Allows Read")
        public Boolean getAllowRead() {
            return allowRead;
        }

        public void setAllowRead(Boolean value) {
            this.allowRead = value;
        }

        @Override
        @ManagementProperty(description="Allows Update")
        public Boolean getAllowUpdate() {
            return allowUpdate;
        }

        public void setAllowUpdate(Boolean value) {
            this.allowUpdate = value;
        }

        @Override
        @ManagementProperty(description="Allows Delete")
        public Boolean getAllowDelete() {
            return allowDelete;
        }

        public void setAllowDelete(Boolean value) {
            this.allowDelete = value;
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
        	return sb.toString();
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
            }        	
            throw new AssertionError();
        }
        
        @Override
        @ManagementProperty(description="Allows Alter")
        public Boolean getAllowAlter() {
			return allowAlter;
		}

        @Override
        @ManagementProperty(description="Allows Execute")
		public Boolean getAllowExecute() {
			return allowExecute;
		}
		
		public void setAllowAlter(Boolean allowAlter) {
			this.allowAlter = allowAlter;
		}
		
		public void setAllowExecute(Boolean allowExecute) {
			this.allowExecute = allowExecute;
		}

		public String toString() {
        	StringBuilder sb = new StringBuilder();
        	sb.append(getResourceName());
        	sb.append("["); //$NON-NLS-1$
        	sb.append(getType());
        	sb.append("]");//$NON-NLS-1$
        	return sb.toString();
        }
	}

    public Boolean isAllowCreateTemporaryTables() {
		return allowCreateTemporaryTables;
	}
    
    public void setAllowCreateTemporaryTables(Boolean allowCreateTemporaryTables) {
		this.allowCreateTemporaryTables = allowCreateTemporaryTables;
	}

    @Override
    @ManagementProperty(description="Indicates if the role is mapped to any authenticated user.")
	public boolean isAnyAuthenticated() {
		return this.anyAuthenticated;
	}
    
    public void setAnyAuthenticated(boolean anyAuthenticated) {
		this.anyAuthenticated = anyAuthenticated;
	}
    
}
