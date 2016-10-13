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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;
import org.teiid.metadata.Grant.Permission;
import org.teiid.metadata.Grant.Permission.Allowance;

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {

	private static final long serialVersionUID = -3130247626435324312L;
	protected NavigableMap<String, Schema> schemas = new TreeMap<String, Schema>(String.CASE_INSENSITIVE_ORDER);
	protected List<Schema> schemaList = new ArrayList<Schema>(); //used for a stable ordering
	protected NavigableMap<String, Datatype> datatypes = new TreeMap<String, Datatype>(String.CASE_INSENSITIVE_ORDER);
	private Map<String, Grant> grants = new TreeMap<String, Grant>(String.CASE_INSENSITIVE_ORDER);
	protected LinkedHashMap<String, Role> roles = new LinkedHashMap<String, Role>();
	
	public NavigableMap<String, Schema> getSchemas() {
		return schemas;
	}
	
	public Schema getSchema(String name) {
		return this.schemas.get(name);
	}
	
	public void addSchema(Schema schema) {
		if (this.schemas.put(schema.getName(), schema) != null) {
			throw new DuplicateRecordException(DataPlugin.Event.TEIID60012, DataPlugin.Util.gs(DataPlugin.Event.TEIID60012, schema.getName()));
		}		
		this.schemaList.add(schema);
	}
	
	public List<Schema> getSchemaList() {
		return schemaList;
	}
	
	public Schema removeSchema(String schemaName) {
	    Schema s = this.schemas.remove(schemaName);
        if ( s != null) {
            this.schemaList.remove(s);
        }       
        return s;
	}
	
	public void addDataTypes(Collection<Datatype> types) {
		if (types != null){
			for (Datatype type:types) {
				addDatatype(type);
			}
		}
	}
	
	public void addDatatype(Datatype datatype) {
		if (!this.datatypes.containsKey(datatype.getName())) {
			this.datatypes.put(datatype.getName(), datatype);
		}
	}
		
	public NavigableMap<String, Datatype> getDatatypes() {
		return datatypes;
	}
	
	public void merge(MetadataStore store) {
		if (store != null) {
			for (Schema s:store.getSchemaList()) {
				addSchema(s);
			}
			addDataTypes(store.getDatatypes().values());
			addGrants(store.grants.values());
		}
	}

	void addGrants(Collection<Grant> grants) {
		if (grants == null) {
			return;
		}
		for (Grant g:grants) {
		    addGrant(g);
		}
	}

	public void addGrant(Grant grant) {
	    if (grant == null) {
	        return;
	    }
	    Grant previous = this.grants.get(grant.getRole());
	    if (previous == null) {
	        this.grants.put(grant.getRole(), grant);
	    } else {
	        for (Permission newP : grant.getPermissions()) {
	            boolean found = false;
	            for (Permission oldP : previous.getPermissions()) {
                    if (oldP.getResourceName().equalsIgnoreCase(newP.getResourceName())
                            && oldP.getResourceType() == newP.getResourceType()) {
	                    oldP.appendAllowances(newP.getAllowances());
	                    found = true;
	                }
	            }
	            if (!found) {
	                previous.addPermission(newP);
	            }
	        }
	    }
	}
	
	public void removeGrant(Grant toRemoveGrant) {
	    if (toRemoveGrant == null) {
	        return;
	    }
	    Grant previous = this.grants.get(toRemoveGrant.getRole());
	    if (previous != null) {
	        for (Permission removePermission : toRemoveGrant.getPermissions()) {
	        	ArrayList<Permission> emptyPermissions = new ArrayList<Permission>();
	            for (Permission currentPermission : previous.getPermissions()) {
                    if (currentPermission.getResourceName().equalsIgnoreCase(removePermission.getResourceName())
                            && currentPermission.getResourceType() == removePermission.getResourceType()) {
                    	boolean all = removePermission.getAllowances().contains(Allowance.ALL_PRIVILEGES);
                    	if (all) {
                    		currentPermission.removeAllowances(currentPermission.getAllowances());
                    	} else {
                    		currentPermission.removeAllowances(removePermission.getAllowances());
                    	}
	                }
                    if (currentPermission.getAllowances().isEmpty()) {
                    	emptyPermissions.add(currentPermission);
                    }
	            }
	            for (Permission p:emptyPermissions) {
	            	previous.removePermission(p);	
	            }
	        }
	        if (previous.getPermissions().isEmpty()) {
	        	this.grants.remove(toRemoveGrant.getRole());
	        }
	    }
	}	
	
	public Collection<Grant> getGrants() {
	    return this.grants.values();
	}
    
    public void addRole(Role role) {
        this.roles.put(role.getName(), role);
    }
    
    public Role getRole(String roleName) {
        return this.roles.get(roleName);
    }

    public Collection<Role> getRoles() {
        return this.roles.values();
    }
    
    public Role removeRole(String roleName) {
        return this.roles.remove(roleName);
    }    
}