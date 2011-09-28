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

import static org.teiid.jboss.Configuration.*;

import java.util.HashMap;
import java.util.Map;

enum Element {
    // must be first
    UNKNOWN(null),

    // VM wide elements
    ASYNC_THREAD_GROUP_ELEMENT(ASYNC_THREAD_GROUP),
    ALLOW_ENV_FUNCTION_ELEMENT(ALLOW_ENV_FUNCTION),
    POLICY_DECIDER_MODULE_ELEMENT(POLICY_DECIDER_MODULE),
	BUFFER_SERVICE_ELEMENT(BUFFER_SERVICE),
	PREPAREDPLAN_CACHE_ELEMENT(PREPAREDPLAN_CACHE),	
	RESULTSET_CACHE_ELEMENT(RESULTSET_CACHE),
    OBJECT_REPLICATOR_ELEMENT(OBJECT_REPLICATOR),
    QUERY_ENGINE_ELEMENT(QUERY_ENGINE),
    	
	// Query-ENGINE
    ENGINE_NAME_ATTRIBUTE(ENGINE_NAME),
	MAX_THREADS_ELEMENT(MAX_THREADS),
	MAX_ACTIVE_PLANS_ELEMENT(MAX_ACTIVE_PLANS),
	USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT(USER_REQUEST_SOURCE_CONCURRENCY),
	TIME_SLICE_IN_MILLI_ELEMENT(TIME_SLICE_IN_MILLI),
	MAX_ROWS_FETCH_SIZE_ELEMENT(MAX_ROWS_FETCH_SIZE),
	LOB_CHUNK_SIZE_IN_KB_ELEMENT(LOB_CHUNK_SIZE_IN_KB),
	AUTHORIZATION_VALIDATOR_MODULE_ELEMENT(AUTHORIZATION_VALIDATOR_MODULE),
	QUERY_THRESHOLD_IN_SECS_ELEMENT(QUERY_THRESHOLD_IN_SECS),
	MAX_SOURCE_ROWS_ELEMENT(MAX_SOURCE_ROWS),
	EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT(EXCEPTION_ON_MAX_SOURCE_ROWS),
	MAX_ODBC_LOB_SIZE_ALLOWED_ELEMENT(MAX_ODBC_LOB_SIZE_ALLOWED),
	DETECTING_CHANGE_EVENTS_ELEMENT(DETECTING_CHANGE_EVENTS),
	MAX_SESSIONS_ALLOWED_ELEMENT(MAX_SESSIONS_ALLOWED),
	SESSION_EXPIRATION_TIME_LIMIT_ELEMENT(SESSION_EXPIRATION_TIME_LIMIT),
	SECURITY_DOMAIN_ELEMENT(SECURITY_DOMAIN),
	JDBC_ELEMENT(JDBC),
	ODBC_ELEMENT(ODBC),	
	
	// buffer manager
	USE_DISK_ELEMENT(USE_DISK, BUFFER_SERVICE),
	PROCESSOR_BATCH_SIZE_ELEMENT(PROCESSOR_BATCH_SIZE, BUFFER_SERVICE),
	CONNECTOR_BATCH_SIZE_ELEMENT(CONNECTOR_BATCH_SIZE, BUFFER_SERVICE),
	MAX_PROCESSING_KB_ELEMENT(MAX_PROCESSING_KB, BUFFER_SERVICE),
	MAX_RESERVED_KB_ELEMENT(MAX_RESERVED_KB, BUFFER_SERVICE),
	MAX_FILE_SIZE_ELEMENT(MAX_FILE_SIZE, BUFFER_SERVICE),
	MAX_BUFFER_SPACE_ELEMENT(MAX_BUFFER_SPACE, BUFFER_SERVICE),
	MAX_OPEN_FILES_ELEMENT(MAX_OPEN_FILES, BUFFER_SERVICE),
	
	//prepared-plan-cache-config
	PPC_MAX_ENTRIES_ATTRIBUTE(MAX_ENTRIES, PREPAREDPLAN_CACHE),
	PPC_MAX_AGE_IN_SECS_ATTRIBUTE(MAX_AGE_IN_SECS, PREPAREDPLAN_CACHE),
	PPC_MAX_STALENESS_ATTRIBUTE(MAX_STALENESS, PREPAREDPLAN_CACHE),
	
	
	// Object Replicator
	OR_STACK_ATTRIBUTE(STACK, OBJECT_REPLICATOR),
	OR_CLUSTER_NAME_ATTRIBUTE(CLUSTER_NAME, OBJECT_REPLICATOR),
	
	// Result set cache
	RSC_ENABLE_ATTRIBUTE(ENABLE, RESULTSET_CACHE),
	RSC_NAME_ELEMENT(NAME, RESULTSET_CACHE),
	RSC_CONTAINER_NAME_ELEMENT(CONTAINER_NAME, RESULTSET_CACHE),
	RSC_MAX_STALENESS_ELEMENT(MAX_STALENESS, RESULTSET_CACHE),
	
	//socket config
	JDBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE(MAX_SOCKET_THREAD_SIZE,JDBC),
	JDBC_IN_BUFFER_SIZE_ATTRIBUTE(IN_BUFFER_SIZE,JDBC),
	JDBC_OUT_BUFFER_SIZE_ATTRIBUTE(OUT_BUFFER_SIZE,JDBC),
	JDBC_SOCKET_BINDING_ATTRIBUTE(SOCKET_BINDING,JDBC),
	
	JDBC_SSL_ELEMENT(SSL, JDBC),
	JDBC_SSL_MODE_ELEMENT(SSL_MODE,JDBC,SSL),
	JDBC_KEY_STORE_FILE_ELEMENT(KEY_STORE_FILE,JDBC,SSL),
	JDBC_KEY_STORE_PASSWD_ELEMENT(KEY_STORE_PASSWD,JDBC,SSL),
	JDBC_KEY_STORE_TYPE_ELEMENT(KEY_STORE_TYPE,JDBC,SSL),
	JDBC_SSL_PROTOCOL_ELEMENT(SSL_PROTOCOL,JDBC,SSL),
	JDBC_KEY_MANAGEMENT_ALG_ELEMENT(KEY_MANAGEMENT_ALG,JDBC,SSL),
	JDBC_TRUST_FILE_ELEMENT(TRUST_FILE,JDBC,SSL),
	JDBC_TRUST_PASSWD_ELEMENT(TRUST_PASSWD,JDBC,SSL),
	JDBC_AUTH_MODE_ELEMENT(AUTH_MODE,JDBC,SSL),
	    
	
	ODBC_MAX_SOCKET_THREAD_SIZE_ATTRIBUTE(MAX_SOCKET_THREAD_SIZE,ODBC),
	ODBC_IN_BUFFER_SIZE_ATTRIBUTE(IN_BUFFER_SIZE,ODBC),
	ODBC_OUT_BUFFER_SIZE_ATTRIBUTE(OUT_BUFFER_SIZE,ODBC),
	ODBC_SOCKET_BINDING_ATTRIBUTE(SOCKET_BINDING,ODBC),
	
	ODBC_SSL_ELEMENT(SSL, ODBC),
	ODBC_SSL_MODE_ELEMENT(SSL_MODE,ODBC,SSL),
	ODBC_KEY_STORE_FILE_ELEMENT(KEY_STORE_FILE,ODBC,SSL),
	ODBC_KEY_STORE_PASSWD_ELEMENT(KEY_STORE_PASSWD,ODBC,SSL),
	ODBC_KEY_STORE_TYPE_ELEMENT(KEY_STORE_TYPE,ODBC,SSL),
	ODBC_SSL_PROTOCOL_ELEMENT(SSL_PROTOCOL,ODBC,SSL),
	ODBC_KEY_MANAGEMENT_ALG_ELEMENT(KEY_MANAGEMENT_ALG,ODBC,SSL),
	ODBC_TRUST_FILE_ELEMENT(TRUST_FILE,ODBC,SSL),
	ODBC_TRUST_PASSWD_ELEMENT(TRUST_PASSWD,ODBC,SSL),
	ODBC_AUTH_MODE_ELEMENT(AUTH_MODE,ODBC,SSL),
	     

	// Translator
    TRANSLATOR_ELEMENT(TRANSLATOR),
    TRANSLATOR_NAME_ATTRIBUTE(TRANSLATOR_NAME),
    TRANSLATOR_MODULE_ATTRIBUTE(TRANSLATOR_MODULE);
    
    private final String name;
    private String[] prefix;

    Element(final String name, String... prefix) {
        this.name = name;
        this.prefix = prefix;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }
    
    public String getModelName() {
    	return buildModelName(this.name, this.prefix);
    }
    
    private static String buildModelName(String name, String... prefix) {
    	if (prefix == null || prefix.length == 0) {
    		return name;
    	}
    	
    	StringBuilder sb = new StringBuilder();
        sb.append(prefix[0]);
        for (int i = 1; i < prefix.length; i++) {
        	sb.append("-"); //$NON-NLS-1$
        	sb.append(prefix[i]);
        }
        sb.append("-"); //$NON-NLS-1$
        sb.append(name);
    	return sb.toString();    	
    }
    
    public String[] getPrefix() {
    	return this.prefix;
    }

    private static final Map<String, Element> elements;

    static {
        final Map<String, Element> map = new HashMap<String, Element>();
        for (Element element : values()) {
            final String name = element.getModelName();
            if (name != null) map.put(name, element);
        }
        elements = map;
    }

    public static Element forName(String localName, String... prefix) {
    	String modelName = buildModelName(localName, prefix);
        final Element element = elements.get(modelName);
        return element == null ? UNKNOWN : element;
    }
}

