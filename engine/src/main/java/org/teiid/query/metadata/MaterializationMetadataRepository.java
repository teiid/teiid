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

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Table;
import org.teiid.query.QueryPlugin;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.TranslatorException;

/**
 * This Metadata repository adds procedures to load materialization views 
 */
public class MaterializationMetadataRepository extends MetadataRepository {
	
	public static final String ALLOW_MATVIEW_MANAGEMENT = "{http://www.teiid.org/ext/relational/2012}ALLOW_MATVIEW_MANAGEMENT";//$NON-NLS-1$
	public static final String ON_VDB_START_SCRIPT = "{http://www.teiid.org/ext/relational/2012}ON_VDB_START_SCRIPT";//$NON-NLS-1$
	public static final String ON_VDB_DROP_SCRIPT = "{http://www.teiid.org/ext/relational/2012}ON_VDB_DROP_SCRIPT";//$NON-NLS-1$
	
	public static final String MATVIEW_AFTER_LOAD_SCRIPT = "{http://www.teiid.org/ext/relational/2012}MATVIEW_AFTER_LOAD_SCRIPT";//$NON-NLS-1$
	public static final String MATVIEW_BEFORE_LOAD_SCRIPT = "{http://www.teiid.org/ext/relational/2012}MATVIEW_BEFORE_LOAD_SCRIPT";//$NON-NLS-1$
	public static final String MATVIEW_LOAD_SCRIPT = "{http://www.teiid.org/ext/relational/2012}MATVIEW_LOAD_SCRIPT";//$NON-NLS-1$
	public static final String MATVIEW_STATUS_TABLE = "{http://www.teiid.org/ext/relational/2012}MATVIEW_STATUS_TABLE"; //$NON-NLS-1$
	public static final String MATVIEW_TTL = "{http://www.teiid.org/ext/relational/2012}MATVIEW_TTL"; //$NON-NLS-1$
	public static final String MATVIEW_STAGE_TABLE = "{http://www.teiid.org/ext/relational/2012}MATERIALIZED_STAGE_TABLE"; //$NON-NLS-1$
	public static final String MATVIEW_SHARE_SCOPE = "{http://www.teiid.org/ext/relational/2012}MATVIEW_SHARE_SCOPE"; //$NON-NLS-1$
	public static final String MATVIEW_ONERROR_ACTION = "{http://www.teiid.org/ext/relational/2012}MATVIEW_ONERROR_ACTION"; //$NON-NLS-1$
	
	public enum LoadStates {NEEDS_LOADING, LOADING, LOADED, FAILED_LOAD};
	public enum Scope {NONE, VDB, SCHEMA};
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
					String statusTable = table.getProperty(MATVIEW_STATUS_TABLE, false); 
					String beforeScript = table.getProperty(MATVIEW_BEFORE_LOAD_SCRIPT, false);		
					String afterScript = table.getProperty(MATVIEW_AFTER_LOAD_SCRIPT, false);
					String stageTable = table.getProperty(MATVIEW_STAGE_TABLE, false);
					String loadScript = table.getProperty(MATVIEW_LOAD_SCRIPT, false);
					
					if (statusTable == null || (stageTable == null && loadScript == null)) {
						throw new TranslatorException(QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31144));
					}
					
					if (beforeScript == null || afterScript == null) {
						LogManager.logWarning(LogConstants.CTX_MATVIEWS, QueryPlugin.Util.gs(QueryPlugin.Event.TEIID31144));
					}
					
				}
				else {
					// internal materialization
				}
			}
		}
	}
}
