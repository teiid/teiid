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
package org.teiid.metadatastore;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.metadata.Schema;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.parser.QueryParser;

public class DeploymentBasedDatabaseStore extends DatabaseStore {
    private VDBRepository vdbRepo;
    
    @SuppressWarnings("serial")
	public static class PendingDataSourceJobs extends HashMap<String, Callable<Boolean>> {}
    
    private ArrayList<VDBImportMetadata> importedVDBs = new ArrayList<VDBImportMetadata>();
    private Map<String, List<ImportedSchema>> importedSchemas = new HashMap<String, List<ImportedSchema>>();
    
    private class ImportedSchema {
    	String foreignSchemaName; 
    	List<String> includeTables;
		List<String> excludeTables;
		Map<String, String> properties;
		String serverType;
    }
    
    public DeploymentBasedDatabaseStore(VDBRepository vdbRepo) {
    	this.vdbRepo = vdbRepo;
    }
    
    @Override
    public Map<String, Datatype> getRuntimeTypes() {
        return vdbRepo.getRuntimeTypeMap();
    }
    @Override
    public Map<String, Datatype> getBuiltinDataTypes() {
        return vdbRepo.getSystemStore().getDatatypes();
    } 

    protected boolean shouldValidateDatabaseBeforeDeploy() {
    	return false;
    }
    
	@Override
	public SystemFunctionManager getSystemFunctionManager() {
		return vdbRepo.getSystemFunctionManager();
	}
	
    public VDBMetaData getVDBMetadata(String contents) {
    	StringReader reader = new StringReader(contents);
    	try {
            startEditing(false);
            QueryParser.getQueryParser().parseDDL(this, new BufferedReader(reader));
        } finally {
        	reader.close();
            stopEditing();
        }
        
        Database database = getDatabases().get(0);
        VDBMetaData vdb = DatabaseUtil.convert(database);
        
        for (ModelMetaData model : vdb.getModelMetaDatas().values()) {
            Schema schema = database.getSchema(model.getName());
            if (this.importedSchemas.get(model.getName()) != null){
                for (ImportedSchema is:this.importedSchemas.get(model.getName())) {
                        model.addProperty("importer.schemaPattern", is.foreignSchemaName);
                        
                        if (is.excludeTables != null && !is.excludeTables.isEmpty()) {
                        	model.addProperty("importer.excludeTables", getCSV(is.excludeTables));
                        }
        
                        // TODO: need to add this to jdbc translator
                        if (is.includeTables != null && !is.includeTables.isEmpty()) {
                        	model.addProperty("importer.includeTables", getCSV(is.includeTables));    
                        }
                        
                        if (is.properties != null) {
                        	for (String key : is.properties.keySet()) {
                        		model.addProperty(key, is.properties.get(key));
                        	}
                        }
                        model.addSourceMetadata(is.serverType, null);
                }
            }
                
            if (!schema.getTables().isEmpty() || !schema.getProcedures().isEmpty()
                    || !schema.getFunctions().isEmpty()) {
                String ddl = DDLStringVisitor.getDDLString(database.getSchema(model.getName()), null, null);
                model.addSourceMetadata("DDL", ddl);
            }
        }  
        
        for (VDBImportMetadata vid : this.importedVDBs) {
        	vdb.getVDBImports().add(vid);
        }
        return vdb;
    }
    
    private String getCSV(List<String> strings) {        
        StringBuilder sb = new StringBuilder();
        if (strings != null && !strings.isEmpty()) {
            for (String str:strings) {
                if (sb.length() > 0) {
                    sb.append(",");                    
                }
                sb.append(str);
            }
        }
        return sb.toString();
    }
    
	@Override
    public void importSchema(String schemaName, String serverType, String serverName, String foreignSchemaName,
            List<String> includeTables, List<String> excludeTables, Map<String, String> properties) {
		ImportedSchema schema = new ImportedSchema();
		schema.foreignSchemaName = foreignSchemaName; 
		schema.includeTables = includeTables;
		schema.excludeTables = excludeTables; 
		schema.properties = properties;
		schema.serverType = serverType;
		
		List<ImportedSchema> imports = importedSchemas.get(schemaName);
		if (imports == null) {
		    imports = new ArrayList<>();
		}
		imports.add(schema);
		this.importedSchemas.put(schemaName, imports);
	}
	
	@Override
	public void importDatabase(String dbName, String version, boolean importPolicies) {
		VDBImportMetadata db = new VDBImportMetadata();
		db.setName(dbName);
		db.setVersion(version);
		db.setImportDataPolicies(importPolicies);
		this.importedVDBs.add(db);
	}
}
