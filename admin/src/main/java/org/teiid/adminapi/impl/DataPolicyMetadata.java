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
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

import org.teiid.adminapi.AdminPlugin;
import org.teiid.adminapi.DataPolicy;
import org.teiid.core.TeiidRuntimeException;


public class DataPolicyMetadata implements DataPolicy, Serializable {
	private static final long serialVersionUID = -4119646357275977190L;
	
    protected String name;
    protected String description;
	protected boolean anyAuthenticated;
	protected Boolean allowCreateTemporaryTables;

    protected Map<String, PermissionMetaData> permissions = new TreeMap<String, PermissionMetaData>(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, PermissionMetaData> languagePermissions = new HashMap<String, PermissionMetaData>(2);
    
    protected List<String> mappedRoleNames = new CopyOnWriteArrayList<String>();
    
    private Set<String> hasRowPermissions = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

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
	
	public boolean hasRowSecurity(String resourceName) {
		return hasRowPermissions.contains(resourceName);
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
        
        private RowSecurityState rowSecurityState;
        
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
        	if (rowSecurityState != null) {
            	if (rowSecurityState.condition != null) {
            		sb.append(" condition ").append(rowSecurityState.condition); //$NON-NLS-1$
            	}
            	if (rowSecurityState.mask != null) {
            		sb.append(" mask ").append(rowSecurityState.mask); //$NON-NLS-1$
            	}
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
    	clone.permissions = new TreeMap<String, PermissionMetaData>(String.CASE_INSENSITIVE_ORDER);
    	clone.permissions.putAll(this.permissions);
    	return clone;
    }
    
}
