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

import org.teiid.api.exception.query.QueryParserException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Table;
import org.teiid.query.parser.QueryParser;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * This Metadata Repository adds/corrects metadata for materialization
 */
public class MaterializationMetadataRepository implements MetadataRepository {

    public static final String ALLOW_MATVIEW_MANAGEMENT = "teiid_rel:ALLOW_MATVIEW_MANAGEMENT";//$NON-NLS-1$
    public static final String ON_VDB_START_SCRIPT = "teiid_rel:ON_VDB_START_SCRIPT";//$NON-NLS-1$
    public static final String ON_VDB_DROP_SCRIPT = "teiid_rel:ON_VDB_DROP_SCRIPT";//$NON-NLS-1$

    public static final String MATVIEW_AFTER_LOAD_SCRIPT = "teiid_rel:MATVIEW_AFTER_LOAD_SCRIPT";//$NON-NLS-1$
    public static final String MATVIEW_BEFORE_LOAD_SCRIPT = "teiid_rel:MATVIEW_BEFORE_LOAD_SCRIPT";//$NON-NLS-1$
    public static final String MATVIEW_LOAD_SCRIPT = "teiid_rel:MATVIEW_LOAD_SCRIPT";//$NON-NLS-1$
    public static final String MATVIEW_STATUS_TABLE = "teiid_rel:MATVIEW_STATUS_TABLE"; //$NON-NLS-1$
    public static final String MATVIEW_TTL = "teiid_rel:MATVIEW_TTL"; //$NON-NLS-1$
    public static final String MATVIEW_STAGE_TABLE = "teiid_rel:MATERIALIZED_STAGE_TABLE"; //$NON-NLS-1$
    public static final String MATVIEW_SHARE_SCOPE = "teiid_rel:MATVIEW_SHARE_SCOPE"; //$NON-NLS-1$
    public static final String MATVIEW_ONERROR_ACTION = "teiid_rel:MATVIEW_ONERROR_ACTION"; //$NON-NLS-1$

    public static final String MATVIEW_UPDATABLE = "teiid_rel:MATVIEW_UPDATABLE"; //$NON-NLS-1$
    public static final String MATVIEW_PREFER_MEMORY = "teiid_rel:MATVIEW_PREFER_MEMORY"; //$NON-NLS-1$
    public static final String MATVIEW_SCOPE = "teiid_rel:MATVIEW_SCOPE"; //$NON-NLS-1$
    public static final String MATVIEW_LOADNUMBER_COLUMN = "teiid_rel:MATVIEW_LOADNUMBER_COLUMN"; //$NON-NLS-1$

    public static final String MATVIEW_PART_LOAD_COLUMN = "teiid_rel:MATVIEW_PART_LOAD_COLUMN"; //$NON-NLS-1$
    public static final String MATVIEW_PART_LOAD_VALUES = "teiid_rel:MATVIEW_PART_LOAD_VALUES"; //$NON-NLS-1$

    public static final String MATVIEW_OWNER_VDB_NAME = "teiid_rel:MATVIEW_OWNER_VDB_NAME"; //$NON-NLS-1$
    public static final String MATVIEW_OWNER_VDB_VERSION = "teiid_rel:MATVIEW_OWNER_VDB_VERSION"; //$NON-NLS-1$

    public static final String MATVIEW_WRITE_THROUGH = "teiid_rel:MATVIEW_WRITE_THROUGH"; //$NON-NLS-1$
    public static final String MATVIEW_MAX_STALENESS_PCT = "teiid_rel:MATVIEW_MAX_STALENESS_PCT"; //$NON-NLS-1$
    public static final String MATVIEW_POLLING_INTERVAL = "teiid_rel:MATVIEW_POLLING_INTERVAL"; //$NON-NLS-1$

    public static final String MATVIEW_POLLING_QUERY = "teiid_rel:MATVIEW_POLLING_QUERY"; //$NON-NLS-1$

    public enum LoadStates {NEEDS_LOADING, LOADING, LOADED, FAILED_LOAD};
    public enum Scope {IMPORTED, FULL};
    public enum ErrorAction {THROW_EXCEPTION, IGNORE, WAIT}
    // Status table column names
    //VDBName, VDBVersion, SchemaName, Name, TargetSchemaName, TargetName, Valid, LoadState, Updated, Cardinality, LoadNumber

    @Override
    public void loadMetadata(MetadataFactory factory, ExecutionFactory executionFactory, Object connectionFactory) throws TranslatorException {
        for (Table table:factory.getSchema().getTables().values()){
            if (table.isMaterialized()) {
                // external materialization
                if (table.getMaterializedTable() != null) {
                    String manage = table.getProperty(ALLOW_MATVIEW_MANAGEMENT, false);
                    if (!Boolean.valueOf(manage)) {
                        continue;
                    }
                    fixScript(ON_VDB_START_SCRIPT, table);
                    fixScript(ON_VDB_DROP_SCRIPT, table);
                    fixScript(MATVIEW_BEFORE_LOAD_SCRIPT, table);
                    fixScript(MATVIEW_AFTER_LOAD_SCRIPT, table);
                    fixScript(MATVIEW_LOAD_SCRIPT, table);
                }
                else {
                    // internal materialization
                }
            }
        }
    }

    /**
     * Rather than require a script to be tokenized directly
     * we expect it to be wrapped in an anon block
     * @param property
     * @param table
     * @return
     */
    private String fixScript(String property, Table table) {
        String script = table.getProperty(property, false);
        if (script == null) {
            return null;
        }
        if (!script.contains(";")) { //$NON-NLS-1$
            return script;
        }
        QueryParser parser = QueryParser.getQueryParser();
        try {
            parser.parseCommand(script);
            return script;
        } catch (QueryParserException e) {
        }
        String wrapped = "begin " + script + "; end"; //$NON-NLS-1$ //$NON-NLS-2$
        try {
            parser.parseCommand(wrapped);
            table.setProperty(property, wrapped);
            return wrapped;
        } catch (QueryParserException e) {
        }
        //because we don't handle empty ; in scripts also try without
        wrapped = "begin " + script + " end"; //$NON-NLS-1$ //$NON-NLS-2$
        try {
            parser.parseCommand(wrapped);
            table.setProperty(property, wrapped);
            return wrapped;
        } catch (QueryParserException e) {
        }
        //give up
        return script;
    }

}
