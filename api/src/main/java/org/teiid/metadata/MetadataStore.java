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

/**
 * Simple holder for metadata.
 */
public class MetadataStore implements Serializable {

    private static final long serialVersionUID = -3130247626435324312L;
    protected NavigableMap<String, Schema> schemas = new TreeMap<String, Schema>(String.CASE_INSENSITIVE_ORDER);
    protected List<Schema> schemaList = new ArrayList<Schema>(); //used for a stable ordering
    protected NavigableMap<String, Datatype> datatypes = new TreeMap<String, Datatype>(String.CASE_INSENSITIVE_ORDER);
    protected NavigableMap<String, Datatype> unmondifiableDatatypes = Collections.unmodifiableNavigableMap(datatypes);
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
            mergeRoles(store.getRoles());
        }
    }

    public void mergeRoles(Collection<Role> toMerge) {
        for (Role r : toMerge) {
            Role existing = this.getRole(r.getName());
            if (existing != null) {
                r.mergeInto(existing);
            } else {
                this.addRole(r);
            }
        }
    }

    void addRole(Role role) {
        this.roles.put(role.getName(), role);
    }

    Role getRole(String roleName) {
        return this.roles.get(roleName);
    }

    public Collection<Role> getRoles() {
        return this.roles.values();
    }

    Role removeRole(String roleName) {
        return this.roles.remove(roleName);
    }
}