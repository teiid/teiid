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
import java.util.List;
import java.util.Map;

import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBImportMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.metadata.Database;
import org.teiid.metadata.Datatype;
import org.teiid.query.function.SystemFunctionManager;
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.parser.QueryParser;

public class DeploymentBasedDatabaseStore extends DatabaseStore {
    private VDBRepository vdbRepo;
    
    private ArrayList<VDBImportMetadata> importedVDBs = new ArrayList<VDBImportMetadata>();
    
    public DeploymentBasedDatabaseStore(VDBRepository vdbRepo) {
    	this.vdbRepo = vdbRepo;
    }
    
    @Override
    public Map<String, Datatype> getRuntimeTypes() {
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
            this.setMode(Mode.DATABASE_STRUCTURE);
            QueryParser.getQueryParser().parseDDL(this, new BufferedReader(reader));
        } finally {
        	reader.close();
            stopEditing();
        }
        
        Database database = getDatabases().get(0);
        VDBMetaData vdb = DatabaseUtil.convert(database);
        
        for (ModelMetaData model : vdb.getModelMetaDatas().values()) {
            model.addSourceMetadata("DDL", null); //$NON-NLS-1$
        }  
        
        for (VDBImportMetadata vid : this.importedVDBs) {
        	vdb.getVDBImports().add(vid);
        }
        
        vdb.addProperty(VDBMetaData.TEIID_DDL, contents);
                
        return vdb;
    }
    
	@Override
    public void importSchema(String schemaName, String serverType, String serverName, String foreignSchemaName,
            List<String> includeTables, List<String> excludeTables, Map<String, String> properties) {
	    if (getSchema(schemaName) == null) {
	        throw new AssertionError();
	    }
	    if (!assertInEditMode(Mode.SCHEMA)) {
            return;
        }
	}
	
	@Override
	public void importDatabase(String dbName, String version, boolean importPolicies) {
	    if (!assertInEditMode(Mode.DATABASE_STRUCTURE)) {
	        return;
	    }
		VDBImportMetadata db = new VDBImportMetadata();
		db.setName(dbName);
		db.setVersion(version);
		db.setImportDataPolicies(importPolicies);
		this.importedVDBs.add(db);
	}
}
