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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.teiid.adminapi.DataPolicy;


public class DataPolicyMetadata implements DataPolicy, Serializable {
	private static final long serialVersionUID = -4119646357275977190L;
	
    protected String name;
    protected String description;
	protected boolean anyAuthenticated;
	protected Boolean allowCreateTemporaryTables;

    protected Map<String, PermissionMetaData> permissions = new TreeMap<String, PermissionMetaData>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, PermissionMetaData> languagePermissions = new HashMap<String, PermissionMetaData>(2);
    
    protected List<String> mappedRoleNames = new CopyOnWriteArrayList<String>();

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
	
	public Map<String, PermissionMetaData> getPermissionMap() {
		return permissions;
	}
	
	public void setPermissions(List<DataPermission> permissions) {
		this.permissions.clear();
		for (DataPermission permission:permissions) {
			addPermissionMetadata((PermissionMetaData)permission);
		}
	}	
	
	public void addPermission(PermissionMetaData... perms) {
		for (PermissionMetaData permission:perms) {
			addPermissionMetadata(permission);
		}
	}

	private void addPermissionMetadata(PermissionMetaData permission) {
		PermissionMetaData previous = null;
		if (permission.getAllowLanguage() != null) {
			previous = this.languagePermissions.put(permission.getResourceName(), permission);
		} else {
			previous = permissions.put(permission.getResourceName().toLowerCase(), permission);
		}
		if (previous != null) {
			permission.bits |= previous.bits;
			permission.bitsSet |= previous.bitsSet;
			if (previous.getCondition() != null) {
				if (permission.getCondition() == null) {
					permission.setCondition(previous.getCondition());
				} else {
					permission.setCondition("(" + permission.getCondition() + ") OR (" + previous.getCondition() + ")"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
				}
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
	
	public Boolean allows(String resourceName, DataPolicy.PermissionType type) {
		PermissionMetaData p = null;
		if (type == PermissionType.LANGUAGE) {
			p = this.languagePermissions.get(resourceName);
		} else {
			p = this.permissions.get(resourceName);
		}
		if (p != null) {
			return p.allows(type);
		}
		return null;
	}
	
	public static class PermissionMetaData implements DataPermission, Serializable {
		private static final long serialVersionUID = 7034744531663164277L;
        
        // XML based fields
        private String resourceName;
        private String condition;
        protected byte bits;
        protected byte bitsSet;
        
        @Override
        public String getResourceName() {
            return resourceName;
        }

        public void setResourceName(String value) {
            this.resourceName = value;
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
	        case LANGUAGE:
	        	return getAllowLanguage();
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
			return condition;
		}
		
		public void setCondition(String filter) {
			this.condition = filter;
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
    
}
