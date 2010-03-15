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

	// The system key is the value used to obtain a connection.
	// In embedded, its a predefined value
	// In enterprise, its the installation directory
	//        public final static String INSTALL_DIR = "install.dir"; //$NON-NLS-1$
	/**
	 * These are global properties used by all components
	 */
	public final static String PROFILE_SERVICE = "ProfileService"; //$NON-NLS-1$

	/**
	 * These properties are exposed via the #getProperty method call.
	 */
	public static String SYSTEM_NAME = "cluster.name"; //$NON-NLS-1$
	public static String SYSTEM_NAME_IDENTIFIER = "JGroups"; //$NON-NLS-1$

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
			public final static String TEIID_RUNTIME_ENGINE = "org.teiid.jboss.deployers.RuntimeEngineDeployer"; //$NON-NLS-1$
			public final static String TEIID_ENGINE_RESOURCE_NAME = "Data Services"; //$NON-NLS-1$
			public final static String TEIID_ENGINE_RESOURCE_DESCRIPTION = "JBoss Enterprise Data Service Runtime Engine"; //$NON-NLS-1$

			public static interface Operations {

				public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$					
				public final static String GET_LONGRUNNINGQUERIES = "listLongRunningQueries"; //$NON-NLS-1$
				public final static String KILL_REQUEST = "cancelRequest"; //$NON-NLS-1$
				public final static String KILL_SESSION = "terminateSession"; //$NON-NLS-1$
				public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
				public final static String GET_REQUESTS = "getRequests"; //$NON-NLS-1$
				public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$

			}

			public static interface Metrics {

				public final static String QUERY_COUNT = "queryCount"; //$NON-NLS-1$            
				public final static String SESSION_COUNT = "sessionCount"; //$NON-NLS-1$
				public final static String LONG_RUNNING_QUERIES = "longRunningQueries"; //$NON-NLS-1$     

			}
		}

		public interface VDB {

			public final static String TYPE = "teiid"; //$NON-NLS-1$
			public final static String SUBTYPE = "vdb"; //$NON-NLS-1$
			public final static String NAME = "Enterprise Virtual Database"; //$NON-NLS-1$
			public final static String DESCRIPTION = "JBoss Enterprise Virtual Database (VDB)"; //$NON-NLS-1$

			public static interface Operations {

				public final static String GET_QUERIES = "listQueries"; //$NON-NLS-1$					
				public final static String GET_LONGRUNNINGQUERIES = "listLongRunningQueries"; //$NON-NLS-1$
				public final static String KILL_REQUEST = "cancelRequest"; //$NON-NLS-1$
				public final static String KILL_SESSION = "terminateSession"; //$NON-NLS-1$
				public final static String GET_PROPERTIES = "getProperties"; //$NON-NLS-1$
				public final static String GET_REQUESTS = "getRequests"; //$NON-NLS-1$
				public final static String GET_SESSIONS = "getSessions"; //$NON-NLS-1$

			}
			
			public static interface Metrics {

				public final static String STATUS = "status"; //$NON-NLS-1$ 
				public final static String QUERY_COUNT = "queryCount"; //$NON-NLS-1$            
				public final static String SESSION_COUNT = "sessionCount"; //$NON-NLS-1$
				public final static String LONG_RUNNING_QUERIES = "longRunningQueries"; //$NON-NLS-1$     

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
			public final static String WAIT_UNTIL_FINISHED = "waitUntilFinished"; //$NON-NLS-1$

			public final static String INCLUDE_SOURCE_QUERIES = "includeSourceQueries"; //$NON-NLS-1$

			public final static String LONG_RUNNING_QUERY_LIMIT = "longRunningQueryLimit"; //$NON-NLS-1$

			public final static String FIELD_LIST = "fieldList"; //$NON-NLS-1$

			public final static String REQUEST_ID = "requestID"; //$NON-NLS-1$
			public final static String SESSION_ID = "sessionID"; //$NON-NLS-1$

			public final static String NAME = "Name"; //$NON-NLS-1$
			public final static String VALUE = "Value"; //$NON-NLS-1$

		}

	}
}
