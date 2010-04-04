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

    @XmlElement(name = "permission")
    protected PermissionMap permissions = new PermissionMap(new KeyBuilder<PermissionMetaData>() {
		private static final long serialVersionUID = -6992984146431492449L;
		@Override
		public String getKey(PermissionMetaData entry) {
			return entry.getResourceName();
		}
	});
    
    @XmlElement(name = "mapped-role-name")
    protected List<String> mappedRoleNames = new ArrayList<String>();

	@Override
	@ManagementProperty(description="Policy Name")
	//@ManagementObjectID(type="policy")
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
		for(PermissionMetaData permission:this.permissions.getMap().values()) {
			if (permission.getResourceName().equalsIgnoreCase(resourceName)  ) {
				return permission.allows(type);
			}
		}
		
		for(PermissionMetaData permission:this.permissions.getMap().values()) {
			if (permission.allows(resourceName, type)) {
				return true;
			}
		}
		return false;
	}
	
	
    @XmlAccessorType(XmlAccessType.NONE)
    @XmlType(name = "", propOrder = {
        "resourceName",
        "allowCreate",
        "allowRead",
        "allowUpdate",
        "allowDelete"
    })	
    @ManagementObject(properties=ManagementProperties.EXPLICIT)
	public static class PermissionMetaData implements DataPermission, Serializable {
		private static final long serialVersionUID = 7034744531663164277L;
		private static final String SEPARATOR = "."; //$NON-NLS-1$
        public static final String RECURSIVE = "*"; //$NON-NLS-1$
        private static final String ALL_NODES = RECURSIVE;
        public static final String SEPARATOR_WITH_RECURSIVE  = SEPARATOR + RECURSIVE;
        
        // derived state
        private String canonicalName; // The resource's canonical name
        private boolean isRecursive = false;  // Is this a recursive resource?
        
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
        
        @Override
        @ManagementProperty(description="Resource Name, for which permission defined")
        //@ManagementObjectID(type="permission")
        @XmlElement(name = "resource-name", required = true)
        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String value) {
            this.resourceName = value;
            init(this.resourceName);
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
        
        public String getType() {
        	StringBuilder sb = new StringBuilder();
        	if (isAllowCreate()) {
        		sb.append("C");//$NON-NLS-1$
        	}
        	if (isAllowRead()) {
        		sb.append("R");//$NON-NLS-1$
        	}
        	if (isAllowUpdate()) {
        		sb.append("U");//$NON-NLS-1$
        	}
        	if (isAllowDelete()) {
        		sb.append("D");//$NON-NLS-1$
        	}     
        	return sb.toString();
        }
        
        public boolean allows(PermissionType type) {
        	boolean allowedType = false;
            switch (type) {
            case CREATE:
            	allowedType = isAllowCreate();
            	break;
            case READ:
            	allowedType = isAllowRead();
            	break;
            case UPDATE:
            	allowedType = isAllowUpdate();
            	break;
            case DELETE:
            	allowedType = isAllowDelete();
            	break;
            }        	
            return allowedType;
        }
        
        public boolean allows(String checkResource, PermissionType type) {
        	boolean allowedType = allows(type);
        	boolean allowed = false;
        	
            if (allowedType) {
            	checkResource = checkResource.toLowerCase();
	        	if ( isRecursive ) {
	                 if ( checkResource.startsWith(this.canonicalName) ) {
	                    allowed = true;
	                 }
	            } else {
	            	allowed = this.canonicalName.equals(checkResource);
	            	
	            	if (!allowed) {
	            		// if this resource is a group level permission, then grant permission to any children
	            		// for ex: 'foo.x.y' has permission if 'foo.x' is defined
	                    int lastSepIndex = checkResource.lastIndexOf(SEPARATOR);
	                    if ( lastSepIndex > 0 && checkResource.substring(0, lastSepIndex).equals(this.canonicalName) ) {
	                        allowed = true;
	                    }
	            	}
	            }
            }
            return allowed;
        }

        /**
         * This method is invoked by the constructors that take a string resource name, and is
         * to strip out any recursive or wildcard characters and return simple the name of the
         * node.
         */
        private void init( String resourceName ) {
            // If the resource name is the ALL_NODES resource ...
            if ( resourceName.equals(ALL_NODES) ) {
                this.isRecursive = true;
                this.canonicalName = "";      // resource name should be nothing //$NON-NLS-1$
            }

            // If the resource name includes the recursive parameter ...
            if ( resourceName.endsWith(SEPARATOR_WITH_RECURSIVE) ) {
                isRecursive = true;
                this.canonicalName = resourceName.substring(0, resourceName.length()-2);
            } else if (resourceName.endsWith(RECURSIVE) ) {
                this.isRecursive = true;
                this.canonicalName = resourceName.substring(0, resourceName.length()-1);
            } else {
                this.canonicalName = resourceName;
            }
            this.canonicalName = this.canonicalName.toLowerCase();
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
}
