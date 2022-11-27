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
package org.teiid.metadatastore;

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
import org.teiid.query.metadata.DatabaseStore;
import org.teiid.query.metadata.DatabaseUtil;
import org.teiid.query.parser.QueryParser;
import org.teiid.runtime.RuntimePlugin;

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

    public VDBMetaData getVDBMetadata(String contents) {
        StringReader reader = new StringReader(contents);
        try {
            startEditing(false);
            this.setMode(Mode.DATABASE_STRUCTURE);
            QueryParser.getQueryParser().parseDDL(this, reader);
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
            throw new AssertionError(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40167, schemaName));
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
