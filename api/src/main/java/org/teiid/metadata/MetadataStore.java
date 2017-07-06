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
			roles.putAll(store.roles);
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

	void addGrant(Grant grant) {
	    if (grant == null) {
	        return;
	    }
	    Grant previous = this.grants.get(grant.getRole());
	    if (previous == null) {
	        this.grants.put(grant.getRole(), grant);
	    } else {
	        for (Permission addPermission : grant.getPermissions()) {
	            boolean found = false;
	            for (Permission currentPermission : new ArrayList<Permission>(previous.getPermissions())) {
                    if (currentPermission.resourceMatches(addPermission)) {
                        found = true;
                        if (addPermission.getMask() != null) {
                            if (currentPermission.getMask() != null) {
                                throw new MetadataException(DataPlugin.Event.TEIID60035, DataPlugin.Util.gs(DataPlugin.Event.TEIID60035, addPermission.getMask(), currentPermission.getMask()));
                            }
                            currentPermission.setMask(addPermission.getMask());
                            currentPermission.setMaskOrder(addPermission.getMaskOrder());
                        }
                        if (addPermission.getCondition() != null) {
                            if (currentPermission.getCondition() != null) {
                                throw new MetadataException(DataPlugin.Event.TEIID60036, DataPlugin.Util.gs(DataPlugin.Event.TEIID60036, addPermission.getMask(), currentPermission.getMask()));
                            }
                            currentPermission.setCondition(addPermission.getCondition(), addPermission.isConditionAConstraint());
                        }
                        currentPermission.appendPrivileges(addPermission.getPrivileges());
                    }
                    if (currentPermission.getPrivileges().isEmpty() 
                            && currentPermission.getRevokePrivileges().isEmpty()
                            && currentPermission.getCondition() == null
                            && currentPermission.getMask() == null) {
                        previous.removePermission(currentPermission);
                    }
                    if (found) {
                        break;
                    }
                }
	            if (!found) {
	                previous.addPermission(addPermission);
	            }
            }
            if (previous.getPermissions().isEmpty()) {
                this.grants.remove(grant.getRole());
            }
	    }
	}
	
	public void removeGrant(Grant toRemoveGrant) {
	    if (toRemoveGrant == null) {
	        return;
	    }
	    Grant previous = this.grants.get(toRemoveGrant.getRole());
	    if (previous == null) {
	        this.grants.put(toRemoveGrant.getRole(), toRemoveGrant);
	    } else {
	        for (Permission revokePermission : toRemoveGrant.getPermissions()) {
                boolean found = false;
                for (Permission currentPermission : new ArrayList<Permission>(previous.getPermissions())) {
                    if (currentPermission.resourceMatches(revokePermission)) {
                        found = true;
                        if (revokePermission.getMask() != null) {
                            if (currentPermission.getMask() != null) {
                                currentPermission.setMask(null);
                                currentPermission.setMaskOrder(null);
                            } else {
                                //TODO: could be exception
                            }
                        }
                        if (revokePermission.getCondition() != null) {
                            if (currentPermission.getCondition() != null) {
                                currentPermission.setCondition(null, null);
                            } else {
                                //TODO: could be exception
                            }
                        }
                        currentPermission.removePrivileges(revokePermission.getRevokePrivileges());
                    }
                    if (currentPermission.getPrivileges().isEmpty() 
                            && currentPermission.getRevokePrivileges().isEmpty()
                            && currentPermission.getCondition() == null
                            && currentPermission.getMask() == null) {
                        previous.removePermission(currentPermission);
                    }
                    if (found) {
                        break;
                    }
                }
                if (!found) {
                    previous.addPermission(revokePermission);
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
    
    void addRole(Role role) {
        this.roles.put(role.getName(), role);
    }
    
    Role getRole(String roleName) {
        return this.roles.get(roleName);
    }

    Collection<Role> getRoles() {
        return this.roles.values();
    }
    
    Role removeRole(String roleName) {
        return this.roles.remove(roleName);
    }    
}