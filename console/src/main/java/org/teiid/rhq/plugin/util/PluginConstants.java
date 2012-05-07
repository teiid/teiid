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
package org.teiid.rhq.plugin.util;

/**
 * These are the Constants that used in conjunction with using the
 * 
 * @since 5.5.3
 */
public interface PluginConstants {

	/**
	 * These are properties required for connecting to the profile service and
	 * getting a handle to a specific component related to Teiid.
	 */

	/**
	 * These are global properties used by all components
	 */
	public final static String PROFILE_SERVICE = "ProfileService"; //$NON-NLS-1$
	
	/**
	 * Log4j log category to use
	 */
	public final static String DEFAULT_LOGGER_CATEGORY = "org.rhq"; //$NON-NLS-1$
	
	
	/**
	 * Use these component type names when calling Connection related methods
	 * that require the type.
	 * 
	 * @since 1.0
	 */
	public interface ComponentType {

		public interface Platform {

			public final static String NAME = "Platform"; //$NON-NLS-1$
			public final static String TEIID_TYPE = "teiid"; //$NON-NLS-1$
			public final static String TEIID_SUB_TYPE = "dqp"; //$NON-NLS-1$
			public final static String TYPE = "ConnectionFactory"; //$NON-NLS-1$
			public final static String SUBTYPE = "NoTx"; //$NON-NLS-1$
			public final static String TEIID_RUNTIME_ENGINE = "RuntimeEngineDeployer"; //$NON-NLS-1$
			public final static String TEIID_ENGINE_RESOURCE_NAME = "Data Services"; //$NON-NLS-1$
			public final static String TEIID_ENGINE_RESOURCE_DESCRIPTION = "Teiid Data Service Runtime Engine"; //$NON-NLS-1$

			public static interface Operations {

				public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$					
				public final static String GET_LONGRUNNINGQUERIES = "getLongRunningRequests"; //$NON-NLS-1$
				public final static String KILL_REQUEST = "cancelRequest"; //$NON-NLS-1$
				public final static String KILL_SESSION = "terminateSession"; //$NON-NLS-1$
				public final static String KILL_TRANSACTION = "terminateTransaction"; //$NON-NLS-1$
				public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
				public final static String GET_REQUESTS = "getRequests"; //$NON-NLS-1$
				public final static String GET_TRANSACTIONS = "getTransactions"; //$NON-NLS-1$
				public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$
				public final static String GET_BUFFER_USAGE = "userBufferSpace"; //$NON-NLS-1$
				public final static String GET_CACHE_STATS = "getCacheStatistics"; //$NON-NLS-1$
				public final static String DEPLOY_VDB_BY_URL = "deployVdbByUrl"; //$NON-NLS-1$
				public final static String VIEW_QUERY_PLAN = "getPlan"; //$NON-NLS-1$
			}

			public static interface Metrics {
				public final static String QUERY_COUNT = "queryCount"; //$NON-NLS-1$            
				public final static String SESSION_COUNT = "sessionCount"; //$NON-NLS-1$
				public final static String LONG_RUNNING_QUERIES = "longRunningQueries"; //$NON-NLS-1$     
				public final static String BUFFER_USAGE = "userBufferSpace"; //$NON-NLS-1$
			}
		}

		public interface VDB {

			public final static String TYPE = "teiid"; //$NON-NLS-1$
			public final static String SUBTYPE = "vdb"; //$NON-NLS-1$
			public final static String NAME = "Teiid Virtual Database"; //$NON-NLS-1$
			public final static String VERSION = "version"; //$NON-NLS-1$
			public final static String DESCRIPTION = "Teiid Virtual Database (VDB)"; //$NON-NLS-1$

			public static interface Operations {

				public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$	
				public final static String CLEAR_CACHE = "clearCache"; //$NON-NLS-1$	
				public final static String EXECUTE_QUERIES = "executeQuery"; //$NON-NLS-1$
				public final static String GET_LONGRUNNINGQUERIES = "getLongRunningRequests"; //$NON-NLS-1$
				public final static String KILL_REQUEST = "cancelRequest"; //$NON-NLS-1$
				public final static String KILL_SESSION = "terminateSession"; //$NON-NLS-1$
				public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
				public final static String GET_REQUESTS = "getRequestsUsingVDB"; //$NON-NLS-1$
				public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$
				public final static String GET_MATVIEWS = "getMaterializedViews"; //$NON-NLS-1$
				public final static String RELOAD_MATVIEW = "reloadMaterializedView"; //$NON-NLS-1$

			}
			
			public static interface Metrics {

				public final static String STATUS = "status"; //$NON-NLS-1$ 
				public final static String QUERY_COUNT = "queryCount"; //$NON-NLS-1$            
				public final static String ERROR_COUNT = "errorCount"; //$NON-NLS-1$
				public final static String SESSION_COUNT = "sessionCount"; //$NON-NLS-1$
				public final static String LONG_RUNNING_QUERIES = "longRunningQueries"; //$NON-NLS-1$     

			}

		}
		
		public interface DATA_ROLE {

			public final static String NAME = "VDB Data Role"; //$NON-NLS-1$
			public final static String DESCRIPTION = "Data/Security Role for a Teiid Virtual Database (VDB)"; //$NON-NLS-1$

			public static interface Operations {
			}
			
			public static interface Metrics {
			}

		}

		public interface Translator {

			public final static String TYPE = "teiid"; //$NON-NLS-1$
			public final static String SUBTYPE = "translator"; //$NON-NLS-1$
			public final static String NAME = "Translator"; //$NON-NLS-1$

			public static interface Operations {

			}
			
			public static interface Metrics {

			}

		}
		public interface Model {

			public final static String TYPE = "teiid"; //$NON-NLS-1$
			public final static String SUBTYPE = "model"; //$NON-NLS-1$
			public final static String NAME = "Model"; //$NON-NLS-1$
			public final static String DESCRIPTION = "Model used to map to a source"; //$NON-NLS-1$

		}

		public interface Connector {

			public final static String TYPE = "ConnectionFactory"; //$NON-NLS-1$
			public final static String SUBTYPE_NOTX = "NoTx"; //$NON-NLS-1$
			public final static String SUBTYPE_TX = "Tx"; //$NON-NLS-1$
			public final static String NAME = "Enterprise Connector"; //$NON-NLS-1$
			public final static String DESCRIPTION = "JBoss Enterprise Connector Binding"; //$NON-NLS-1$

			public static interface Operations {

				public final static String RESTART_CONNECTOR = "restart"; //$NON-NLS-1$            
				public final static String STOP_CONNECTOR = "stop"; //$NON-NLS-1$ 

			}

		}

		public interface Session {

			public final static String TYPE = "Runtime.Sesssion"; //$NON-NLS-1$

			public static interface Query {

				public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$
			}
		}

		public interface Queries {

			public final static String TYPE = "Runtime.Queries"; //$NON-NLS-1$

			public static interface Query {

				public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$
			}
		}

	}

	/**
	 * Use these metric names when calling getValues() on the connection
	 * interface.
	 * 
	 * @since 1.0
	 */
	public interface Metric {
		public final static String HIGH_WATER_MARK = "highWatermark"; //$NON-NLS-1$

	}

	/**
	 * Use these operation names when calling executeOperation() on the
	 * connection interface.
	 * 
	 * @since 1.0
	 */
	public static interface Operation {
		public final static String KILL_REQUEST = "killRequest"; //$NON-NLS-1$
		public final static String GET_VDBS = "listVDBs"; //$NON-NLS-1$
		public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
		public final static String GET_REQUESTS = "getRequests"; //$NON-NLS-1$
		public final static String GET_SESSIONS = "getActiveSessions"; //$NON-NLS-1$

		/**
		 * Use these value names when calling executeOperation() on the
		 * connection interface. These will correlate with parameters used in
		 * operations.
		 * 
		 * @since 1.0
		 */
		public static interface Value {
			public final static String STOP_NOW = "stopNow"; //$NON-NLS-1$  
			public final static String MAT_VIEW_QUERY = "select SchemaName, Name, TargetSchemaName, TargetName, " + //$NON-NLS-1$ 
														"Valid, LoadState, Updated, Cardinality from SYSADMIN.MATVIEWS " +  //$NON-NLS-1$  
														"where SchemaName != 'pg_catalog'"; //$NON-NLS-1$  
			public final static String MAT_VIEW_REFRESH = "exec SYSADMIN.refreshMatView('param1','param2');";  //$NON-NLS-1$
			public final static String WAIT_UNTIL_FINISHED = "waitUntilFinished"; //$NON-NLS-1$

			public final static String INCLUDE_SOURCE_QUERIES = "includeSourceQueries"; //$NON-NLS-1$

			public final static String LONG_RUNNING_QUERY_LIMIT = "longRunningQueryLimit"; //$NON-NLS-1$

			public final static String FIELD_LIST = "fieldList"; //$NON-NLS-1$
			public final static String TRANSACTION_ID = "transactionID"; //$NON-NLS-1$
			public final static String REQUEST_ID = "requestID"; //$NON-NLS-1$
			public final static String SESSION_ID = "sessionID"; //$NON-NLS-1$
			public final static String VDB_URL = "vdbUrl"; //$NON-NLS-1$
			public final static String VDB_DEPLOY_NAME = "vdbDeployName"; //$NON-NLS-1$
			public final static String VDB_VERSION = "vdbVersion"; //$NON-NLS-1$
			public final static String NAME = "Name"; //$NON-NLS-1$
			public final static String VALUE = "Value"; //$NON-NLS-1$
			public final static String MATVIEW_SCHEMA = "schema"; //$NON-NLS-1$
			public final static String MATVIEW_TABLE = "table"; //$NON-NLS-1$
			public final static String INVALIDATE_MATVIEW = "invalidate"; //$NON-NLS-1$
			public final static String CACHE_TYPE = "cacheType"; //$NON-NLS-1$

		}

	}
}
