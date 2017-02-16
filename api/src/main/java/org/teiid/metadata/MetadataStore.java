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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;
import org.teiid.metadata.Grant.Permission;
import org.teiid.metadata.Grant.Permission.Privilege;

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {

	private static final long serialVersionUID = -3130247626435324312L;
	protected NavigableMap<String, Schema> schemas = new TreeMap<String, Schema>(String.CASE_INSENSITIVE_ORDER);
	protected List<Schema> schemaList = new ArrayList<Schema>(); //used for a stable ordering
	protected NavigableMap<String, Datatype> datatypes = new TreeMap<String, Datatype>(String.CASE_INSENSITIVE_ORDER);
	protected NavigableMap<String, Datatype> unmondifiableDatatypes = Collections.unmodifiableNavigableMap(datatypes);
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
	
	public void addDataTypes(Map<String, Datatype> typeMap) {
		if (typeMap != null){
			for (Map.Entry<String, Datatype> entry:typeMap.entrySet()) {
				addDatatype(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public void addDatatype(String name, Datatype datatype) {
		if (!this.datatypes.containsKey(name)) {
			this.datatypes.put(name, datatype);
		}
	}
		
	public NavigableMap<String, Datatype> getDatatypes() {
		return unmondifiableDatatypes;
	}
	
	/**
	 * Get the type information excluding aliases and case sensitive by name
	 * @return
	 */
	public NavigableMap<String, Datatype> getDatatypesExcludingAliases() {
        TreeMap<String, Datatype> result = new TreeMap<String, Datatype>();
        for (Map.Entry<String, Datatype> entry : this.datatypes.entrySet()) {
            if (entry.getKey().equals(entry.getValue().getName())) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
	
	public void merge(MetadataStore store) {
		if (store != null) {
			for (Schema s:store.getSchemaList()) {
				addSchema(s);
			}
			addDataTypes(store.getDatatypes());
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
	                    oldP.appendPrivileges(newP.getPrivileges());
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
                    	boolean all = removePermission.getPrivileges().contains(Privilege.ALL_PRIVILEGES);
                    	if (all) {
                    		currentPermission.removePrivileges(currentPermission.getPrivileges());
                    	} else {
                    		currentPermission.removePrivileges(removePermission.getPrivileges());
                    	}
	                }
                    if (currentPermission.getPrivileges().isEmpty()) {
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