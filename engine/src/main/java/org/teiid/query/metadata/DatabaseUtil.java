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
package org.teiid.query.metadata;

import java.util.Collection;
import java.util.List;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.DataPermission;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.util.StringUtil;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Permission;
import org.teiid.metadata.Permission.Privilege;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;
import org.teiid.metadata.Table;

public class DatabaseUtil {

    public static Database convert(VDBMetaData vdb, MetadataStore metadataStore) {
        Database db = new Database(vdb.getName(), vdb.getVersion());
        db.setProperties(vdb.getPropertiesMap());
        if (vdb.getDescription() != null) {
            db.setAnnotation(vdb.getDescription());
        }
        if (ConnectionType.BY_VERSION != vdb.getConnectionType()) {
            db.setProperty("connection-type", vdb.getConnectionType().name());
        }

        db.getMetadataStore().addDataTypes(metadataStore.getDatatypes());

        // override translators
        List<Translator> translators = vdb.getOverrideTranslators();
        for (Translator t: translators) {
            // add the base
            if (db.getDataWrapper(t.getType()) == null) {
                DataWrapper dw = new DataWrapper(t.getType());
                db.addDataWrapper(dw);
            }
            // add override with properties
            if (db.getDataWrapper(t.getName()) == null) {
                DataWrapper dw = new DataWrapper(t.getName());
                dw.setType(t.getType());
                for (final String key : t.getProperties().stringPropertyNames()) {
                    dw.setProperty(key, t.getPropertyValue(key));
                }
                if (t.getDescription() != null) {
                    dw.setAnnotation(t.getDescription());
                }
                db.addDataWrapper(dw);
            }
        }

        Collection<ModelMetaData> models = vdb.getModelMetaDatas().values();
        for (ModelMetaData m:models) {
            Schema schema = metadataStore.getSchema(m.getName());

            // add servers
            if (m.isSource()){
                Collection<SourceMappingMetadata> sources = m.getSourceMappings();


                for (SourceMappingMetadata s: sources) {
                    // add translators, that are not override
                    if (db.getDataWrapper(s.getTranslatorName()) == null) {
                        DataWrapper dw = new DataWrapper(s.getTranslatorName());
                        db.addDataWrapper(dw);
                    }

                    // add servers
                    Server server = new Server(s.getName());
                    server.setResourceName(s.getConnectionJndiName());
                    server.setDataWrapper(s.getTranslatorName());
                    // no need to add duplicate definitions.
                    if (db.getServer(s.getName()) == null) {
                        db.addServer(server);
                        schema.addServer(server);
                    }
                }
            }

            db.addSchema(schema);
        }

        for (String key : vdb.getDataPolicyMap().keySet()) {
            DataPolicyMetadata dpm = vdb.getDataPolicyMap().get(key);
            Role role = new Role(dpm.getName());
            if (dpm.getMappedRoleNames() != null && !dpm.getMappedRoleNames().isEmpty()) {
                role.setMappedRoles(dpm.getMappedRoleNames());
            }

            if (dpm.isAnyAuthenticated()) {
                role.setAnyAuthenticated(true);
            }

            if (dpm.isGrantAll()) {
                Permission permission = new Permission();
                permission.setAllowAllPrivileges(true);
                permission.setResourceType(ResourceType.DATABASE);
                role.addGrant(permission);
            }

            if (dpm.isAllowCreateTemporaryTables() != null && dpm.isAllowCreateTemporaryTables()) {
                Permission permission = new Permission();
                permission.setAllowTemporyTables(true);
                permission.setResourceType(ResourceType.DATABASE);
                role.addGrant(permission);
            }

            for (DataPolicy.DataPermission dp: dpm.getPermissions()) {
                Permission permission = convert(dp, metadataStore);
                role.addGrant(permission);
            }
            db.addRole(role);
        }
        return db;
    }

    private static Permission convert(DataPermission dp, MetadataStore store) {
        Permission p = new Permission();

        p.setAllowAlter(dp.getAllowAlter());
        p.setAllowDelete(dp.getAllowDelete());
        p.setAllowExecute(dp.getAllowExecute());
        p.setAllowInsert(dp.getAllowCreate());
        p.setAllowSelect(dp.getAllowRead());
        p.setAllowUpdate(dp.getAllowUpdate());
        p.setResourceName(dp.getResourceName());

        if (dp.getAllowLanguage() != null) {
            p.setAllowUsage(true);
            p.setResourceType(ResourceType.LANGUAGE);
        } else if (dp.getResourceType() != null) {
            p.setResourceType(ResourceType.valueOf(dp.getResourceType().name()));
        } else {
            List<String> parts = StringUtil.split(dp.getResourceName(), "."); //$NON-NLS-1$

            if (parts.size() == 1) {
                p.setResourceType(ResourceType.SCHEMA);
            } else {
                if (dp.getMask() != null && parts.size() > 2) {
                    p.setResourceType(ResourceType.COLUMN);
                }
                Schema s = store.getSchema(parts.get(0));
                if (s != null) {
                    if (parts.size() > 2) {
                        ResourceType fullType = getType(s, StringUtil.join(parts.subList(1, parts.size()), ".")); //$NON-NLS-1$
                        ResourceType type = getType(s, StringUtil.join(parts.subList(1, parts.size()-1), ".")); //$NON-NLS-1$
                        if (fullType != null && type == null) {
                            p.setResourceType(fullType);
                        } else if (type != null && fullType == null) {
                            if (type == ResourceType.TABLE) {
                                p.setResourceType(ResourceType.COLUMN);
                            }
                        }
                    } else {
                        String name = parts.get(1);

                        ResourceType type = getType(s, name);
                        if (type != null) {
                            p.setResourceType(type);
                        }
                    }
                }
            }
        }

        if (dp.getMask() != null) {
            p.setMask(dp.getMask());
            p.setMaskOrder(dp.getOrder());
        }

        if (dp.getCondition() != null) {
            p.setCondition(dp.getCondition(), dp.getConstraint());
        }
        return p;
    }

    private static ResourceType getType(Schema s, String name) {
        Table t = s.getTable(name);
        Procedure proc = s.getProcedure(name);
        boolean hasFunction = s.getFunctions().values().stream().anyMatch(f->(name.equalsIgnoreCase(f.getName())));

        if (t != null && proc == null && !hasFunction) {
            return ResourceType.TABLE;
        } else if (proc != null && t == null && !hasFunction) {
            return ResourceType.PROCEDURE;
        } else if (hasFunction && t == null && proc == null) {
            return ResourceType.FUNCTION;
        }

        return null;
    }

    public static VDBMetaData convert(Database database) {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName(database.getName());
        vdb.setVersion(database.getVersion());
        vdb.setDescription(database.getAnnotation());

        if (database.getProperty("connection-type", false) != null) {
            vdb.setConnectionType(VDB.ConnectionType.valueOf(database.getProperty("connection-type", false)));
        }
        vdb.getPropertiesMap().putAll(database.getProperties());

        String domainDDLString = DDLStringVisitor.getDomainDDLString(database);
        if (!domainDDLString.isEmpty()) {
            vdb.addProperty(VDBMetaData.TEIID_DOMAINS, domainDDLString);
        }

        // translators
        for (DataWrapper dw : database.getDataWrappers()) {
            if (dw.getType() == null) {
                // we only care about the override types in the VDB
                continue;
            }

            VDBTranslatorMetaData translator = new VDBTranslatorMetaData();
            translator.setName(dw.getName());
            translator.setType(dw.getType());
            translator.setDescription(dw.getAnnotation());
            translator.getPropertiesMap().putAll(dw.getProperties());
            vdb.addOverideTranslator(translator);
        }

        for(Schema schema : database.getSchemas()) {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName(schema.getName());
            mmd.setDescription(schema.getAnnotation());
            mmd.setVisible(Boolean.valueOf(schema.isVisible()));
            mmd.getPropertiesMap().putAll(schema.getProperties());
            if (schema.isPhysical()) {
                mmd.setModelType(Model.Type.PHYSICAL);

                for (Server server : schema.getServers()) {
                    // if there are more properties to create DS they will be lost in this translation
                    String connectionName = server.getResourceName();
                    if (connectionName == null) {
                        connectionName = server.getName();
                    }
                    mmd.addSourceMapping(server.getName(), server.getDataWrapper(), connectionName);
                }
            } else {
                mmd.setModelType(Model.Type.VIRTUAL);
            }
            vdb.addModel(mmd);
        }

        copyDatabaseGrantsAndRoles(database, vdb);

        return vdb;
    }

    public static void copyDatabaseGrantsAndRoles(Database database,
            VDBMetaData vdb) {
        // roles
        for (Role role : database.getRoles()) {
            //there will be placeholder roles from the structure parsing, we just simply replace them
            DataPolicyMetadata dpm = convert(role, null);
            vdb.addDataPolicy(dpm);
        }
    }

    public static PermissionMetaData convert(Permission from) {
        PermissionMetaData pmd = new PermissionMetaData();
        pmd.setResourceName(from.getResourceName());
        pmd.setResourceType(DataPolicy.ResourceType.valueOf(from.getResourceType().name()));
        pmd.setAllowAlter(from.hasPrivilege(Privilege.ALTER));
        pmd.setAllowCreate(from.hasPrivilege(Privilege.INSERT));
        pmd.setAllowDelete(from.hasPrivilege(Privilege.DELETE));
        pmd.setAllowExecute(from.hasPrivilege(Privilege.EXECUTE));
        pmd.setAllowRead(from.hasPrivilege(Privilege.SELECT));
        pmd.setAllowUpdate(from.hasPrivilege(Privilege.UPDATE));
        pmd.setAllowLanguage(from.hasPrivilege(Privilege.USAGE));

        pmd.setCondition(from.getCondition());
        pmd.setConstraint(from.isConditionAConstraint());
        pmd.setMask(from.getMask());
        pmd.setOrder(from.getMaskOrder());

        return pmd;
    }

    public static DataPolicyMetadata convert(Role role, DataPolicyMetadata dpm) {
        if (dpm == null) {
            dpm = new DataPolicyMetadata();
            dpm.setName(role.getName());

            dpm.setDescription(role.getAnnotation());

            if (role.getMappedRoles() != null && !role.getMappedRoles().isEmpty()) {
                dpm.setMappedRoleNames(role.getMappedRoles());
            }
        }

        if (role.isAnyAuthenticated()) {
            dpm.setAnyAuthenticated(true);
        }

        for (Permission grant:role.getGrants().values()) {
            if (Boolean.TRUE.equals(grant.hasPrivilege(Privilege.ALL_PRIVILEGES))) {
                dpm.setGrantAll(true);
                continue;
            } else if (Boolean.TRUE.equals(grant.hasPrivilege(Privilege.TEMPORARY_TABLE))) {
                dpm.setAllowCreateTemporaryTables(true);
                continue;
            }

            PermissionMetaData pmd = convert(grant);
            dpm.addPermission(pmd);
        }

        dpm.addPolicies(role.getPolicies());

        return dpm;
    }

}
