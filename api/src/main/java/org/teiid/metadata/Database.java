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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import org.teiid.connector.DataPlugin;
import org.teiid.core.types.DataTypeManager;
import org.teiid.metadata.BaseColumn.NullType;

public class Database extends AbstractMetadataRecord {
    private static final long serialVersionUID = 7595765832848232840L;
    public enum ResourceType {DATABASE, SCHEMA, TABLE, PROCEDURE, FUNCTION, COLUMN, SERVER,
        DATAWRAPPER, PARAMETER, ROLE, GRANT, LANGUAGE, POLICY};
    protected MetadataStore store = new MetadataStore();
    protected NavigableMap<String, DataWrapper> wrappers = new TreeMap<String, DataWrapper>(String.CASE_INSENSITIVE_ORDER);
    protected NavigableMap<String, Server> servers = new TreeMap<String, Server>(String.CASE_INSENSITIVE_ORDER);
    private String version;
    private NamespaceContainer namespaceContainer = new NamespaceContainer();

    public Database(String dbName) {
        super.setName(dbName);
        this.version = "1";
    }

    public Database(String dbName, String version) {
        super.setName(dbName);
        this.version = version;
    }

    public void addSchema(Schema schema) {
        Schema s = this.store.getSchema(schema.getName());
        if ( s != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60021,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60021, schema.getName()));
        } else {
            this.store.addSchema(schema);
        }
    }

    public Schema getSchema(String schemaName) {
        return this.store.getSchema(schemaName);
    }

    public List<Schema> getSchemas() {
        return new ArrayList<Schema>(this.store.getSchemaList());
    }

    public Schema removeSchema(String schemaName) {
        Schema s = getSchema(schemaName);
        if (s == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60024,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60024, schemaName));
        }
        return this.store.removeSchema(schemaName);
    }

    public MetadataStore getMetadataStore() {
        return this.store;
    }

    public String getVersion() {
        return this.version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public void addDataWrapper(DataWrapper wrapper) {
        DataWrapper w = this.wrappers.get(wrapper.getName());
        if (w != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60022,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60022, wrapper.getName()));
        } else {
            this.wrappers.put(wrapper.getName(), wrapper);
        }
    }

    public DataWrapper removeDataWrapper(String wrapperName) {
        DataWrapper wrapper = this.wrappers.get(wrapperName);
        if (wrapper == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60023,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60023, wrapperName));
        }
        return this.wrappers.remove(wrapperName);
    }

    public DataWrapper getDataWrapper(String wrapperName) {
        return this.wrappers.get(wrapperName);
    }

    public List<DataWrapper> getDataWrappers(){
        return new ArrayList<DataWrapper>(this.wrappers.values());
    }

    public void addServer(Server server) {
        Server s = this.servers.get(server.getName());
        if (s != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60026,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60026, server.getName()));
        } else {
            this.servers.put(server.getName(), server);
        }
    }

    public Server removeServer(String serverName) {
        Server server = this.servers.get(serverName);
        if (server == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60027,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60027, serverName));
        }
        return this.servers.remove(serverName);
    }

    public Server getServer(String serverName) {
        return this.servers.get(serverName);
    }

    public List<Server> getServers() {
        return new ArrayList<Server>(this.servers.values());
    }

    public String resolveNamespaceInPropertyKey(String key) {
        return NamespaceContainer.resolvePropertyKey(key);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + ((getName() == null) ? 0 : getName().hashCode());
        result = prime * result + ((version == null) ? 0 : version.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (!super.equals(obj))
            return false;
        if (getClass() != obj.getClass())
            return false;
        Database other = (Database) obj;
        if (getName() == null) {
            if (other.getName() != null)
                return false;
        } else if (!getName().equals(other.getName()))
            return false;
        if (version == null) {
            if (other.version != null)
                return false;
        } else if (!version.equals(other.version))
            return false;
        return true;
    }

    public Role getRole(String roleName) {
        return store.getRole(roleName);
    }

    public Collection<Role> getRoles() {
        return store.getRoles();
    }

    public void addRole(Role role) {
        Role r  = this.store.getRole(role.getName());
        if ( r != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60028,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60028, role.getName()));
        }
        this.store.addRole(role);
    }

    public void removeRole(String roleName) {
        Role r = findRole(roleName);
        // make sure it is not used in any grants
        if (!r.getGrants().isEmpty() || !r.getPolicies().isEmpty()) {
            throw new MetadataException(DataPlugin.Event.TEIID60030,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60030, roleName));
        }
        this.store.removeRole(roleName);
    }

    public Role findRole(String roleName) {
        Role r = this.store.getRole(roleName);
        if (r == null) {
            throw new MetadataException(DataPlugin.Event.TEIID60029,
                    DataPlugin.Util.gs(DataPlugin.Event.TEIID60029, roleName));
        }
        return r;
    }

    public void addGrant(Grant grant) {
        Role r = findRole(grant.getRole());
        r.addGrant(grant.getPermission());
    }

    public void revokeGrant(Grant grant) {
        Role r = findRole(grant.getRole());
        r.removeGrant(grant.getPermission());
    }

    public void addPolicy(String roleName, Policy policy) {
        Role r = findRole(roleName);
        r.addPolicy(policy);
    }

    public void removePolicy(String roleName, Policy policy) {
        Role r = findRole(roleName);
        r.removePolicy(policy);
    }

    /**
     * Add a domain with the given attributes.  The UID must still be set.
     * @param name
     * @param baseType
     * @param precision
     * @param scale
     * @param notNull
     * @return
     */
    public Datatype addDomain(String name, String baseType, Integer precision, Integer scale, boolean notNull) {
        //TODO: allow named array types
        // requires either storing the dimension on the datatype, or using a holder
        /*int dimensions = 0;
        while (DataTypeManager.isArrayType(baseType)) {
            baseType = DataTypeManager.getComponentType(baseType);
            dimensions++;
        }*/
        Datatype base = store.getDatatypes().get(baseType);
        if (base == null || !base.isBuiltin()) {
            throw new MetadataException(DataPlugin.Event.TEIID60032, DataPlugin.Util.gs(DataPlugin.Event.TEIID60032, baseType));
        }
        Datatype existing = store.getDatatypes().get(name);
        if (existing != null) {
            throw new DuplicateRecordException(DataPlugin.Event.TEIID60034, DataPlugin.Util.gs(DataPlugin.Event.TEIID60032, existing));
        }
        Datatype dataType = base.clone();
        dataType.setName(name);
        dataType.setBasetypeName(baseType);
        dataType.setType(Datatype.Type.Domain);
        dataType.setUUID(null);

        if (precision != null) {
            if (!Number.class.isAssignableFrom(DataTypeManager.getDataTypeClass(base.getRuntimeTypeName()))) {
                dataType.setLength(precision);
            } else {
                dataType.setPrecision(precision);
            }
        }
        if (scale != null) {
            dataType.setScale(scale);
        }
        if (notNull) {
            dataType.setNullType(NullType.No_Nulls);
        }

        store.addDatatype(name, dataType);
        return dataType;
    }

    public void addNamespace(String prefix, String uri) {
        this.namespaceContainer.addNamespace(prefix, uri);
    }

    public NamespaceContainer getNamespaceContainer() {
        return namespaceContainer;
    }

}
