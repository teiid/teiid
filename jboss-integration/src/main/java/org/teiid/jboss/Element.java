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

import java.util.HashMap;
import java.util.Map;
import static org.teiid.jboss.Configuration.*;

enum Element {
    // must be first
    UNKNOWN(null),
    QUERY_ENGINE_ELEMENT(QUERY_ENGINE),
    	
	// Query-ENGINE
    ASYNC_THREAD_GROUP_ELEMENT(ASYNC_THREAD_GROUP),
	MAX_THREADS_ELEMENT(MAX_THREADS),
	MAX_ACTIVE_PLANS_ELEMENT(MAX_ACTIVE_PLANS),
	USER_REQUEST_SOURCE_CONCURRENCY_ELEMENT(USER_REQUEST_SOURCE_CONCURRENCY),
	TIME_SLICE_IN_MILLI_ELEMENT(TIME_SLICE_IN_MILLI),
	MAX_ROWS_FETCH_SIZE_ELEMENT(MAX_ROWS_FETCH_SIZE),
	LOB_CHUNK_SIZE_IN_KB_ELEMENT(LOB_CHUNK_SIZE_IN_KB),
	USE_DATA_ROLES_ELEMENT(USE_DATA_ROLES),
	ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT_ELEMENT(ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT),
	ALLOW_FUNCTION_CALLS_BY_DEFAULT_ELEMENT(ALLOW_FUNCTION_CALLS_BY_DEFAULT),
	QUERY_THRESHOLD_IN_SECS_ELEMENT(QUERY_THRESHOLD_IN_SECS),
	MAX_SOURCE_ROWS_ELEMENT(MAX_SOURCE_ROWS),
	EXCEPTION_ON_MAX_SOURCE_ROWS_ELEMENT(EXCEPTION_ON_MAX_SOURCE_ROWS),
	MAX_ODBC_LOB_SIZE_ALLOWED_ELEMENT(MAX_ODBC_LOB_SIZE_ALLOWED),
	EVENT_DISTRIBUTOR_NAME_ELEMENT(EVENT_DISTRIBUTOR_NAME),
	DETECTING_CHANGE_EVENTS_ELEMENT(DETECTING_CHANGE_EVENTS),
	JDBC_SECURITY_DOMAIN_ELEMENT(SECURITY_DOMAIN),
	MAX_SESSIONS_ALLOWED_ELEMENT(MAX_SESSIONS_ALLOWED),
	SESSION_EXPIRATION_TIME_LIMIT_ELEMENT(SESSION_EXPIRATION_TIME_LIMIT),
	ALLOW_ENV_FUNCTION_ELEMENT(ALLOW_ENV_FUNCTION),
	
	//children
	BUFFER_SERVICE_ELEMENT(BUFFER_SERVICE),
	RESULTSET_CACHE_ELEMENT(RESULTSET_CACHE),
	PREPAREDPLAN_CACHE_ELEMENT(PREPAREDPLAN_CACHE),	
	CACHE_FACORY_ELEMENT(CACHE_FACORY),
	JDBC_ELEMENT(JDBC),
	ODBC_ELEMENT(ODBC),	
	
	// buffer manager
	USE_DISK_ELEMENT(USE_DISK),
	PROCESSOR_BATCH_SIZE_ELEMENT(PROCESSOR_BATCH_SIZE),
	CONNECTOR_BATCH_SIZE_ELEMENT(CONNECTOR_BATCH_SIZE),
	MAX_RESERVE_BATCH_COLUMNS_ELEMENT(MAX_RESERVE_BATCH_COLUMNS),
	MAX_PROCESSING_BATCH_COLUMNS_ELEMENT(MAX_PROCESSING_BATCH_COLUMNS),
	MAX_FILE_SIZE_ELEMENT(MAX_FILE_SIZE),
	MAX_BUFFER_SPACE_ELEMENT(MAX_BUFFER_SPACE),
	MAX_OPEN_FILES_ELEMENT(MAX_OPEN_FILES),
	
	//cache-config
	MAX_ENTRIES_ELEMENT(MAX_ENTRIES),
	MAX_AGE_IN_SECS_ELEMENT(MAX_AGE_IN_SECS),
	MAX_STALENESS_ELEMENT(MAX_STALENESS),
	CACHE_TYPE_ELEMENT(CACHE_TYPE),
	CACHE_LOCATION_ELEMENT(CACHE_LOCATION),
	
	// cache-factory
	 CACHE_SERVICE_JNDI_NAME_ELEMENT(CACHE_SERVICE_JNDI_NAME),
	 RESULTSET_CACHE_NAME_ELEMENT(RESULTSET_CACHE_NAME),
	
	//socket config
	MAX_SOCKET_SIZE_ELEMENT(MAX_SOCKET_THREAD_SIZE),
	IN_BUFFER_SIZE_ELEMENT(IN_BUFFER_SIZE),
	OUT_BUFFER_SIZE_ELEMENT(OUT_BUFFER_SIZE),
	SOCKET_BINDING_ELEMENT(SOCKET_BINDING),
	SOCKET_ENABLED_ELEMENT(SOCKET_ENABLED),
	SSL_MODE_ELEMENT(SSL_MODE),
	KEY_STORE_FILE_ELEMENT(KEY_STORE_FILE),
	KEY_STORE_PASSWD_ELEMENT(KEY_STORE_PASSWD),
	KEY_STORE_TYPE_ELEMENT(KEY_STORE_TYPE),
	SSL_PROTOCOL_ELEMENT(SSL_PROTOCOL),
	KEY_MANAGEMENT_ALG_ELEMENT(KEY_MANAGEMENT_ALG),
	TRUST_FILE_ELEMENT(TRUST_FILE),
	TRUST_PASSWD_ELEMENT(TRUST_PASSWD),
	AUTH_MODE_ELEMENT(AUTH_MODE),
	SSL_ELEMENT(SSL);    
    
    private final String name;

    Element(final String name) {
        this.name = name;
    }

    /**
     * Get the local name of this element.
     *
     * @return the local name
     */
    public String getLocalName() {
        return name;
    }

    private static final Map<String, Element> elements;

    static {
        final Map<String, Element> map = new HashMap<String, Element>();
        for (Element element : values()) {
            final String name = element.getLocalName();
            if (name != null) map.put(name, element);
        }
        elements = map;
    }

    public static Element forName(String localName) {
        final Element element = elements.get(localName);
        return element == null ? UNKNOWN : element;
    }
}

