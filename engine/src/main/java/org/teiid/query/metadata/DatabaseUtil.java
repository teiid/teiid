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
package org.teiid.query.metadata;

import java.util.Collection;
import java.util.List;

import org.teiid.adminapi.DataPolicy;
import org.teiid.adminapi.DataPolicy.DataPermission;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.adminapi.impl.DataPolicyMetadata.PermissionMetaData;
import org.teiid.metadata.DataWrapper;
import org.teiid.metadata.Database;
import org.teiid.metadata.Database.ResourceType;
import org.teiid.metadata.Grant;
import org.teiid.metadata.Grant.Permission;
import org.teiid.metadata.Grant.Permission.Allowance;
import org.teiid.metadata.MetadataStore;
import org.teiid.metadata.Role;
import org.teiid.metadata.Schema;
import org.teiid.metadata.Server;

public class DatabaseUtil {

    public static Database convert(VDBMetaData vdb, MetadataStore metadataStore) {                 
        Database db = new Database(vdb.getName(), vdb.getVersion());
        db.setProperties(vdb.getPropertiesMap());
        if (vdb.getDescription() != null) {
            db.setAnnotation(vdb.getDescription());
        }
        db.setProperty("connection-type", vdb.getConnectionType().name());
        
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
	                if (s.getConnectionJndiName() != null) {
	                    server.setJndiName(s.getConnectionJndiName());
	                } else {
	                    server.setType("NONE");
	                }
	                server.setDataWrapper(s.getTranslatorName());
	                // no need to add duplicate definitions.
	                if (db.getServer(s.getName()) == null) {
	                	db.addServer(server);
	                	schema.addServer(server);
	                }
	            }
            }
            
            if (m.getDescription() != null) {
                schema.setAnnotation(m.getDescription());
            }
            
            if(!m.isVisible()) {
                schema.setVisible(false);
            }
            db.addSchema(schema);
        }
        
        for (String key : vdb.getDataPolicyMap().keySet()) {
            DataPolicyMetadata dpm = vdb.getDataPolicyMap().get(key);
            Role role = new Role(dpm.getName());
            if (dpm.getMappedRoleNames() != null && !dpm.getMappedRoleNames().isEmpty()) {
                role.setJaasRoles(dpm.getMappedRoleNames());
            }
            
            if (dpm.isAnyAuthenticated()) {
                role.setAnyAuthenticated(true);
            }

            Grant grant = null;         
            if (dpm.isGrantAll()) {
                if (grant == null) {
                    grant = new Grant();
                    grant.setRole(role.getName());
                }
                Permission permission = new Permission();
                permission.setAllowAllPrivileges(true);
                permission.setResourceType(ResourceType.DATABASE);
                grant.addPermission(permission);
            }
            
            if (dpm.isAllowCreateTemporaryTables() != null && dpm.isAllowCreateTemporaryTables()) {
                if (grant == null) {
                    grant = new Grant();
                    grant.setRole(role.getName());
                }
                Permission permission = new Permission();
                permission.setAllowTemporyTables(true);
                permission.setResourceType(ResourceType.DATABASE);
                grant.addPermission(permission);                
            }
            
            for (DataPolicy.DataPermission dp: dpm.getPermissions()) {
                if (grant == null) {
                    grant = new Grant();
                    grant.setRole(role.getName());
                }
                
                Permission permission = convert(dp);
                grant.addPermission(permission);
            }
            db.addRole(role);
            db.addGrant(grant);
        }
        return db;
    }
    
    private static Permission convert(DataPermission dp) {
        Permission p = new Permission();
        p.setResourceType(ResourceType.TABLE);
        
        p.setAllowAlter(dp.getAllowAlter());
        p.setAllowDelete(dp.getAllowDelete());
        p.setAllowDrop(false);
        p.setAllowExecute(dp.getAllowExecute());
        p.setAllowInsert(dp.getAllowCreate());
        p.setAllowLanguage(dp.getAllowLanguage());
        p.setAllowSelect(dp.getAllowRead());
        p.setAllowUpdate(dp.getAllowUpdate());
        p.setResourceName(dp.getResourceName());
        
        int dotCount = dp.getResourceName().length() - dp.getResourceName().replaceAll("\\.", "").length();
        
        // this is more of a guessing game here..
        if (dp.getAllowLanguage() != null && dp.getAllowLanguage()) {
            p.setResourceType(ResourceType.DATABASE);
        } else if (dotCount == 0) {
            p.setResourceType(ResourceType.SCHEMA);
        } else if (dp.getAllowExecute() != null && dp.getAllowExecute()){
            p.setResourceType(ResourceType.PROCEDURE);
        } else if (dotCount >= 2 ) {
            p.setResourceType(ResourceType.COLUMN);
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

    public static VDBMetaData convert(Database database) {
        VDBMetaData vdb = new VDBMetaData();
        vdb.setName(database.getName());
        vdb.setVersion(database.getVersion());
        vdb.setDescription(database.getAnnotation());
        
        if (database.getProperty("connection-type", false) != null) {
            vdb.setConnectionType(VDB.ConnectionType.valueOf(database.getProperty("connection-type", false)));
        }
        vdb.getPropertiesMap().putAll(database.getProperties());
        
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
            
            if (schema.isPhysical()) {
                mmd.setModelType(Model.Type.PHYSICAL);
                
                for (Server server : schema.getServers()) {
                    // if there are more properties to create DS they will be lost in this translation
                    String connectionName = server.getJndiName();
                    if (connectionName == null) {
                        connectionName = server.getName();
                    }
                    if (server.getType().equalsIgnoreCase("NONE")) {
                        mmd.addSourceMapping(server.getName(), server.getDataWrapper(), null);
                    } else {
                        mmd.addSourceMapping(server.getName(), server.getDataWrapper(), connectionName);
                    }
                }
            } else {
                mmd.setModelType(Model.Type.VIRTUAL);
            }
            vdb.addModel(mmd);
        }
        
        // roles
        for (Grant grant:database.getGrants()) {
            Role role = database.getRole(grant.getRole());
            DataPolicyMetadata dpm = convert(grant, role);
            vdb.addDataPolicy(dpm);
        }
        return vdb;
    }
    
    static PermissionMetaData convert(Permission from) {
        PermissionMetaData pmd = new PermissionMetaData();
        pmd.setResourceName(from.getResourceName());
        
        // NOTE: PMD even though you set false, it can interpret it wrong, use null or true only
        if (from.hasAllowance(Allowance.ALTER)) {
            pmd.setAllowAlter(true);
        }
        if (from.hasAllowance(Allowance.INSERT)) {
            pmd.setAllowCreate(true);
        }
        if (from.hasAllowance(Allowance.DELETE)) {
            pmd.setAllowDelete(true);
        }
        if(from.hasAllowance(Allowance.EXECUTE)) {
            pmd.setAllowExecute(true);
        }
        if (from.hasAllowance(Allowance.SELECT)) {
            pmd.setAllowRead(true);
        }
        if(from.hasAllowance(Allowance.UPDATE)) {
            pmd.setAllowUpdate(true);
        }
        if (from.hasAllowance(Allowance.LANGUAGE)) {
            pmd.setAllowLanguage(true);
        }
        
        pmd.setCondition(from.getCondition());
        pmd.setConstraint(from.isConditionAConstraint());
        pmd.setMask(from.getMask());
        pmd.setOrder(from.getMaskOrder());

        return pmd;
    }
    
    static DataPolicyMetadata convert(Grant from, Role role) {
        DataPolicyMetadata dpm = new DataPolicyMetadata();
        dpm.setName(from.getRole());
        
        for (Permission p : from.getPermissions()) {
            if (p.hasAllowance(Allowance.ALL_PRIVILEGES)) {
                dpm.setGrantAll(true);
                continue;
            } else if (p.hasAllowance(Allowance.TEMPORARY_TABLE)) {
                dpm.setAllowCreateTemporaryTables(true);
                continue;
            }
            
            PermissionMetaData pmd = convert(p);            
            dpm.addPermission(pmd);
        }
        
        if (role != null) {
            dpm.setDescription(role.getAnnotation());
        }
        
        if (role != null && role.getJassRoles() != null && !role.getJassRoles().isEmpty()) {
            dpm.setMappedRoleNames(role.getJassRoles());
        }
        
        if (role.isAnyAuthenticated()) {
            dpm.setAnyAuthenticated(true);
        }
        
        return dpm;
    }    
}
