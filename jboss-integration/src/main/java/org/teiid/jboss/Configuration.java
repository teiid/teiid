/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2011, Red Hat, Inc., and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss;

class Configuration {
	public static final String BUFFER_SERVICE = "buffer-service";//$NON-NLS-1$
	public static final String RESULTSET_CACHE = "resultset-cache";//$NON-NLS-1$
	public static final String PREPAREDPLAN_CACHE = "preparedplan-cache";//$NON-NLS-1$	
	public static final String CACHE_FACORY = "distributed-cache-factory";//$NON-NLS-1$
	public static final String QUERY_ENGINE = "query-engine";//$NON-NLS-1$
	public static final String JDBC = "jdbc";//$NON-NLS-1$
	public static final String ODBC = "odbc"; //$NON-NLS-1$
	
	// Query-ENGINE
	public static final String JNDI_NAME = "jndi-name"; //$NON-NLS-1$
	public static final String MAX_THREADS = "maxThreads";//$NON-NLS-1$
	public static final String MAX_ACTIVE_PLANS = "maxActivePlans";//$NON-NLS-1$
	public static final String USER_REQUEST_SOURCE_CONCURRENCY = "userRequestSourceConcurrency";//$NON-NLS-1$
	public static final String TIME_SLICE_IN_MILLI = "timeSliceInMilli";//$NON-NLS-1$
	public static final String MAX_ROWS_FETCH_SIZE = "maxRowsFetchSize";//$NON-NLS-1$
	public static final String LOB_CHUNK_SIZE_IN_KB = "lobChunkSizeInKB";//$NON-NLS-1$
	public static final String USE_DATA_ROLES = "useDataRoles";//$NON-NLS-1$
	public static final String ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT = "allowCreateTemporaryTablesByDefault";//$NON-NLS-1$
	public static final String ALLOW_FUNCTION_CALLS_BY_DEFAULT = "allowFunctionCallsByDefault";//$NON-NLS-1$
	public static final String QUERY_THRESHOLD_IN_SECS = "queryThresholdInSecs";//$NON-NLS-1$
	public static final String MAX_SOURCE_ROWS = "maxSourceRows";//$NON-NLS-1$
	public static final String EXCEPTION_ON_MAX_SOURCE_ROWS = "exceptionOnMaxSourceRows";//$NON-NLS-1$
	public static final String MAX_ODBC_LOB_SIZE_ALLOWED = "maxODBCLobSizeAllowed";//$NON-NLS-1$
	public static final String EVENT_DISTRIBUTOR_NAME = "eventDistributorName";//$NON-NLS-1$
	public static final String DETECTING_CHANGE_EVENTS = "detectingChangeEvents";//$NON-NLS-1$
	public static final String JDBC_SECURITY_DOMAIN = "jdbc-security-domain";//$NON-NLS-1$
	public static final String MAX_SESSIONS_ALLOWED = "max-sessions-allowed";//$NON-NLS-1$
	public static final String SESSION_EXPIRATION_TIME_LIMIT = "sessions-expiration-timelimit";//$NON-NLS-1$
	public static final String ALLOW_ENV_FUNCTION = "allow-env-function";//$NON-NLS-1$
	
	public static final String USE_DISK = "useDisk";//$NON-NLS-1$
	public static final String DISK_DIRECTORY = "diskDirectory";//$NON-NLS-1$
	public static final String PROCESSOR_BATCH_SIZE = "processorBatchSize";//$NON-NLS-1$
	public static final String CONNECTOR_BATCH_SIZE = "connectorBatchSize";//$NON-NLS-1$
	public static final String MAX_RESERVE_BATCH_COLUMNS = "maxReserveBatchColumns";//$NON-NLS-1$
	public static final String MAX_PROCESSING_BATCH_COLUMNS = "maxProcessingBatchesColumns";//$NON-NLS-1$
	public static final String MAX_FILE_SIZE = "maxFileSize";//$NON-NLS-1$
	public static final String MAX_BUFFER_SPACE = "maxBufferSpace";//$NON-NLS-1$
	public static final String MAX_OPEN_FILES = "maxOpenFiles";//$NON-NLS-1$
		
	//cache-config
	public static final String ENABLED = "enabled";//$NON-NLS-1$
	public static final String MAX_ENTRIES = "maxEntries";//$NON-NLS-1$
	public static final String MAX_AGE_IN_SECS = "maxAgeInSeconds";//$NON-NLS-1$
	public static final String MAX_STALENESS = "maxStaleness";//$NON-NLS-1$
	public static final String CACHE_TYPE = "type";//$NON-NLS-1$
	public static final String CACHE_LOCATION= "location";//$NON-NLS-1$
	
	// cache-factory
	public static final String  CACHE_SERVICE_JNDI_NAME = "cache-service-jndi-name";//$NON-NLS-1$
	public static final String  RESULTSET_CACHE_NAME = "resultsetCacheName";//$NON-NLS-1$
	
	//socket config
	public static final String MAX_SOCKET_THREAD_SIZE = "maxSocketThreads";//$NON-NLS-1$
	public static final String IN_BUFFER_SIZE = "inputBufferSize";//$NON-NLS-1$
	public static final String OUT_BUFFER_SIZE = "outputBufferSize";//$NON-NLS-1$
	public static final String SOCKET_BINDING = "socket-binding";//$NON-NLS-1$
	public static final String SOCKET_ENABLED = "enabled";//$NON-NLS-1$
	public static final String SSL_MODE = "mode";//$NON-NLS-1$
	public static final String KEY_STORE_FILE = "keystoreFilename";//$NON-NLS-1$
	public static final String KEY_STORE_PASSWD = "keystorePassword";//$NON-NLS-1$
	public static final String KEY_STORE_TYPE = "keystoreType";//$NON-NLS-1$
	public static final String SSL_PROTOCOL = "sslProtocol";//$NON-NLS-1$
	public static final String KEY_MANAGEMENT_ALG = "keymanagementAlgorithm";//$NON-NLS-1$
	public static final String TRUST_FILE = "truststoreFilename";//$NON-NLS-1$
	public static final String TRUST_PASSWD = "truststorePassword";//$NON-NLS-1$
	public static final String AUTH_MODE = "authenticationMode";//$NON-NLS-1$
	public static final String SSL = "ssl";//$NON-NLS-1$
}


