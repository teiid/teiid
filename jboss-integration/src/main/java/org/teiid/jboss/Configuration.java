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

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

class Configuration {
	public static final String BUFFER_SERVICE = "buffer-service";//$NON-NLS-1$
	public static final String PREPAREDPLAN_CACHE = "preparedplan-cache";//$NON-NLS-1$	
	public static final String RESULTSET_CACHE = "resultset-cache";//$NON-NLS-1$
	public static final String QUERY_ENGINE = "query-engine";//$NON-NLS-1$
	public static final String JDBC = "jdbc";//$NON-NLS-1$
	public static final String ODBC = "odbc"; //$NON-NLS-1$
	public static final String TRANSLATOR = "translator"; //$NON-NLS-1$
	
	// Query-ENGINE
	public static final String ASYNC_THREAD_GROUP = "async-thread-group";//$NON-NLS-1$
	public static final String MAX_THREADS = "max-threads";//$NON-NLS-1$
	public static final String MAX_ACTIVE_PLANS = "max-active-plans";//$NON-NLS-1$
	public static final String USER_REQUEST_SOURCE_CONCURRENCY = "thread-count-for-source-concurrency";//$NON-NLS-1$
	public static final String TIME_SLICE_IN_MILLI = "time-slice-in-millseconds";//$NON-NLS-1$
	public static final String MAX_ROWS_FETCH_SIZE = "max-row-fetch-size";//$NON-NLS-1$
	public static final String LOB_CHUNK_SIZE_IN_KB = "lob-chunk-size-in-kb";//$NON-NLS-1$
	public static final String QUERY_THRESHOLD_IN_SECS = "query-threshold-in-seconds";//$NON-NLS-1$
	public static final String MAX_SOURCE_ROWS = "max-source-rows-allowed";//$NON-NLS-1$
	public static final String EXCEPTION_ON_MAX_SOURCE_ROWS = "exception-on-max-source-rows";//$NON-NLS-1$
	public static final String MAX_ODBC_LOB_SIZE_ALLOWED = "max-odbc-lob-size-allowed";//$NON-NLS-1$
	public static final String OBJECT_REPLICATOR = "object-replicator";//$NON-NLS-1$
	public static final String DETECTING_CHANGE_EVENTS = "detect-change-events";//$NON-NLS-1$
	public static final String SECURITY_DOMAIN = "security-domain";//$NON-NLS-1$
	public static final String MAX_SESSIONS_ALLOWED = "max-sessions-allowed";//$NON-NLS-1$
	public static final String SESSION_EXPIRATION_TIME_LIMIT = "sessions-expiration-timelimit";//$NON-NLS-1$
	public static final String ALLOW_ENV_FUNCTION = "allow-env-function";//$NON-NLS-1$
	public static final String AUTHORIZATION_VALIDATOR_MODULE = "authorization-validator-module"; //$NON-NLS-1$
	public static final String POLICY_DECIDER_MODULE = "policy-decider-module"; //$NON-NLS-1$
	
	
	// Buffer Manager
	public static final String USE_DISK = "use-disk";//$NON-NLS-1$
	public static final String PROCESSOR_BATCH_SIZE = "processor-batch-size";//$NON-NLS-1$
	public static final String CONNECTOR_BATCH_SIZE = "connector-batch-size";//$NON-NLS-1$
	public static final String MAX_PROCESSING_KB = "max-processing-kb";//$NON-NLS-1$
	public static final String MAX_RESERVED_KB = "max-reserve-kb";//$NON-NLS-1$
	public static final String MAX_FILE_SIZE = "max-file-size";//$NON-NLS-1$
	public static final String MAX_BUFFER_SPACE = "max-buffer-space";//$NON-NLS-1$
	public static final String MAX_OPEN_FILES = "max-open-files";//$NON-NLS-1$
	
	//cache-config
	public static final String MAX_ENTRIES = "max-entries";//$NON-NLS-1$
	public static final String MAX_AGE_IN_SECS = "max-age-in-seconds";//$NON-NLS-1$
	public static final String MAX_STALENESS = "max-staleness";//$NON-NLS-1$
	public static final String ENABLE = "enable";//$NON-NLS-1$
	
	// cache-container
	public static final String  NAME = "name";//$NON-NLS-1$
	public static final String  CONTAINER_NAME = "container-name";//$NON-NLS-1$
	
	//socket config
	public static final String MAX_SOCKET_THREAD_SIZE = "max-socket-threads";//$NON-NLS-1$
	public static final String IN_BUFFER_SIZE = "input-buffer-size";//$NON-NLS-1$
	public static final String OUT_BUFFER_SIZE = "output-buffer-size";//$NON-NLS-1$
	public static final String SOCKET_BINDING = "socket-binding";//$NON-NLS-1$
	public static final String SOCKET_ENABLED = "enabled";//$NON-NLS-1$
	public static final String SSL_MODE = "mode";//$NON-NLS-1$
	public static final String KEY_STORE_FILE = "keystore-name";//$NON-NLS-1$
	public static final String KEY_STORE_PASSWD = "keystore-password";//$NON-NLS-1$
	public static final String KEY_STORE_TYPE = "keystore-type";//$NON-NLS-1$
	public static final String SSL_PROTOCOL = "ssl-protocol";//$NON-NLS-1$
	public static final String KEY_MANAGEMENT_ALG = "keymanagement-algorithm";//$NON-NLS-1$
	public static final String TRUST_FILE = "truststore-name";//$NON-NLS-1$
	public static final String TRUST_PASSWD = "truststore-password";//$NON-NLS-1$
	public static final String AUTH_MODE = "authentication-mode";//$NON-NLS-1$
	public static final String SSL = "ssl";//$NON-NLS-1$
	
	public static final String TRANSLATOR_NAME = "name";//$NON-NLS-1$
	public static final String TRANSLATOR_MODULE = "module";//$NON-NLS-1$
	
	public static final String STACK = "stack";//$NON-NLS-1$
	public static final String CLUSTER_NAME = "cluster-name";//$NON-NLS-1$
	
	public static final String ENGINE_NAME = "name";//$NON-NLS-1$
	
	public static final String DESC = ".describe"; //$NON-NLS-1$
	static void addAttribute(ModelNode node, String name, String type, String description, ModelType dataType, boolean required, String defaultValue) {
		node.get(type, name, TYPE).set(dataType);
        node.get(type, name, DESCRIPTION).set(description);
        node.get(type, name, REQUIRED).set(required);
        node.get(type, name, MAX_OCCURS).set(1);
        if (defaultValue != null) {
        	if (ModelType.INT.equals(dataType)) {
        		node.get(type, name, DEFAULT).set(Integer.parseInt(defaultValue));
        	}
        	else if (ModelType.BOOLEAN.equals(dataType)) {
        		node.get(type, name, DEFAULT).set(Boolean.parseBoolean(defaultValue));
        	}
        	else if (ModelType.LONG.equals(dataType)) {
        		node.get(type, name, DEFAULT).set(Long.parseLong(defaultValue));
        	}        	
        	else {
        		node.get(type, name, DEFAULT).set(defaultValue);
        	}
        }
        //TODO: add "allowed" values
    }	
}


