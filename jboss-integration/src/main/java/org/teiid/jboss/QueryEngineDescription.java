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
package org.teiid.jboss;

import static org.jboss.as.controller.descriptions.ModelDescriptionConstants.*;
import static org.teiid.jboss.Configuration.*;

import java.util.Locale;
import java.util.ResourceBundle;

import org.jboss.as.controller.descriptions.DescriptionProvider;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

class QueryEngineDescription implements DescriptionProvider {
	
	
	@Override
	public ModelNode getModelDescription(Locale locale) {
		final ResourceBundle bundle = IntegrationPlugin.getResourceBundle(locale);
		
        final ModelNode node = new ModelNode();
        node.get(OPERATION_NAME).set(ADD);
        node.get(DESCRIPTION).set("susbsystem.add"); //$NON-NLS-1$
        
        getQueryEngineDescription(node.get(CHILDREN, Configuration.QUERY_ENGINE), REQUEST_PROPERTIES, bundle);
        return node;
	}
		
	static void getQueryEngineDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.JNDI_NAME, type, bundle.getString(Configuration.JNDI_NAME+DESC), ModelType.STRING, true, "teiid/engine-deployer"); //$NON-NLS-1$
		addAttribute(node, Configuration.ASYNC_THREAD_GROUP, type, bundle.getString(Configuration.ASYNC_THREAD_GROUP+DESC), ModelType.STRING, false, "teiid-async"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_THREADS, type, bundle.getString(Configuration.MAX_THREADS+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ACTIVE_PLANS, type, bundle.getString(Configuration.MAX_ACTIVE_PLANS+DESC), ModelType.INT, false, "20"); //$NON-NLS-1$
		addAttribute(node, Configuration.USER_REQUEST_SOURCE_CONCURRENCY, type, bundle.getString(Configuration.USER_REQUEST_SOURCE_CONCURRENCY+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.TIME_SLICE_IN_MILLI, type, bundle.getString(Configuration.TIME_SLICE_IN_MILLI+DESC), ModelType.INT, false, "2000"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_ROWS_FETCH_SIZE, type, bundle.getString(Configuration.MAX_ROWS_FETCH_SIZE+DESC), ModelType.INT, false, "20480"); //$NON-NLS-1$
		addAttribute(node, Configuration.LOB_CHUNK_SIZE_IN_KB, type, bundle.getString(Configuration.LOB_CHUNK_SIZE_IN_KB+DESC), ModelType.INT, false, "100"); //$NON-NLS-1$
		addAttribute(node, Configuration.USE_DATA_ROLES, type, bundle.getString(Configuration.USE_DATA_ROLES+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT, type, bundle.getString(Configuration.ALLOW_CREATE_TEMPORY_TABLES_BY_DEFAULT+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT, type, bundle.getString(Configuration.ALLOW_FUNCTION_CALLS_BY_DEFAULT+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.QUERY_THRESHOLD_IN_SECS, type, bundle.getString(Configuration.QUERY_THRESHOLD_IN_SECS+DESC), ModelType.INT, false, "600"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_SOURCE_ROWS, type, bundle.getString(Configuration.MAX_SOURCE_ROWS+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS, type, bundle.getString(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ODBC_LOB_SIZE_ALLOWED, type, bundle.getString(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED+DESC), ModelType.INT, false, "5242880"); //$NON-NLS-1$
		addAttribute(node, Configuration.EVENT_DISTRIBUTOR_NAME, type, bundle.getString(Configuration.EVENT_DISTRIBUTOR_NAME+DESC), ModelType.STRING, false, "teiid/event-distributor"); //$NON-NLS-1$
		addAttribute(node, Configuration.DETECTING_CHANGE_EVENTS, type, bundle.getString(Configuration.DETECTING_CHANGE_EVENTS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		
		//session stuff
		addAttribute(node, Configuration.JDBC_SECURITY_DOMAIN, type, bundle.getString(Configuration.JDBC_SECURITY_DOMAIN+DESC), ModelType.STRING, false, null);
		addAttribute(node, Configuration.MAX_SESSIONS_ALLOWED, type, bundle.getString(Configuration.MAX_SESSIONS_ALLOWED+DESC), ModelType.INT, false, "5000"); //$NON-NLS-1$
		addAttribute(node, Configuration.SESSION_EXPIRATION_TIME_LIMIT, type, bundle.getString(Configuration.SESSION_EXPIRATION_TIME_LIMIT+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		
		addAttribute(node, Configuration.ALLOW_ENV_FUNCTION, type, bundle.getString(Configuration.ALLOW_ENV_FUNCTION+DESC), ModelType.BOOLEAN, false, "false"); //$NON-NLS-1$
		
		//Buffer Manager stuff
		ModelNode bufferNode = node.get(CHILDREN, Configuration.BUFFER_SERVICE);
		bufferNode.get(TYPE).set(ModelType.OBJECT);
		bufferNode.get(DESCRIPTION).set(bundle.getString(Configuration.BUFFER_SERVICE+DESC));
		bufferNode.get(REQUIRED).set(false);
		bufferNode.get(MAX_OCCURS).set(1);
		bufferNode.get(MIN_OCCURS).set(1);
		getBufferDescription(bufferNode, type, bundle);
		
		// result-set-cache
		ModelNode rsCacheNode = node.get(CHILDREN, Configuration.RESULTSET_CACHE);
		rsCacheNode.get(TYPE).set(ModelType.OBJECT);
		rsCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.RESULTSET_CACHE+DESC));
		rsCacheNode.get(REQUIRED).set(false);
		rsCacheNode.get(MAX_OCCURS).set(1);
		rsCacheNode.get(MIN_OCCURS).set(1);
		getResultsetCacheDescription(rsCacheNode, type, bundle);
		
		// preparedplan-set-cache
		ModelNode preparedPlanCacheNode = node.get(CHILDREN, Configuration.PREPAREDPLAN_CACHE);
		preparedPlanCacheNode.get(TYPE).set(ModelType.OBJECT);
		preparedPlanCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.PREPAREDPLAN_CACHE+DESC));
		preparedPlanCacheNode.get(REQUIRED).set(false);
		preparedPlanCacheNode.get(MAX_OCCURS).set(1);
		preparedPlanCacheNode.get(MIN_OCCURS).set(1);
		getResultsetCacheDescription(preparedPlanCacheNode, type, bundle);
		
		//distributed-cache
		ModelNode distributedCacheNode = node.get(CHILDREN, Configuration.CACHE_FACORY);
		distributedCacheNode.get(TYPE).set(ModelType.OBJECT);
		distributedCacheNode.get(DESCRIPTION).set(bundle.getString(Configuration.CACHE_FACORY+DESC));
		distributedCacheNode.get(REQUIRED).set(false);
		distributedCacheNode.get(MAX_OCCURS).set(1);
		distributedCacheNode.get(MIN_OCCURS).set(1);
		getDistributedCacheDescription(preparedPlanCacheNode, type, bundle);
		
		//jdbc
		ModelNode jdbcSocketNode = node.get(CHILDREN, Configuration.JDBC);
		jdbcSocketNode.get(TYPE).set(ModelType.OBJECT);
		jdbcSocketNode.get(DESCRIPTION).set(bundle.getString(Configuration.JDBC+DESC));
		jdbcSocketNode.get(REQUIRED).set(false);
		jdbcSocketNode.get(MAX_OCCURS).set(1);
		jdbcSocketNode.get(MIN_OCCURS).set(1);	
		getSocketConfig(jdbcSocketNode, type, bundle);
		
		//odbc
		ModelNode odbcSocketNode = node.get(CHILDREN, Configuration.ODBC);
		odbcSocketNode.get(TYPE).set(ModelType.OBJECT);
		odbcSocketNode.get(DESCRIPTION).set(bundle.getString(Configuration.ODBC+DESC));
		odbcSocketNode.get(REQUIRED).set(false);
		odbcSocketNode.get(MAX_OCCURS).set(1);
		odbcSocketNode.get(MIN_OCCURS).set(1);	
		getSocketConfig(odbcSocketNode, type, bundle);			
	}
	
	private static void getDistributedCacheDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.ENABLED, type, bundle.getString(Configuration.ENABLED+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_SERVICE_JNDI_NAME, type, bundle.getString(Configuration.CACHE_SERVICE_JNDI_NAME+DESC), ModelType.STRING, false, "java:TeiidCacheManager"); //$NON-NLS-1$
		addAttribute(node, Configuration.RESULTSET_CACHE_NAME, type, bundle.getString(Configuration.RESULTSET_CACHE_NAME+DESC), ModelType.STRING, false, "teiid-resultset-cache"); //$NON-NLS-1$
	}
	
	private static void getBufferDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.USE_DISK, type, bundle.getString(Configuration.USE_DISK+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.DISK_DIRECTORY, type, bundle.getString(Configuration.DISK_DIRECTORY+DESC), ModelType.STRING, true, null); 
		addAttribute(node, Configuration.PROCESSOR_BATCH_SIZE, type, bundle.getString(Configuration.PROCESSOR_BATCH_SIZE+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, Configuration.CONNECTOR_BATCH_SIZE, type, bundle.getString(Configuration.CONNECTOR_BATCH_SIZE+DESC), ModelType.INT, false, "1024"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_RESERVE_BATCH_COLUMNS, type, bundle.getString(Configuration.MAX_RESERVE_BATCH_COLUMNS+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_PROCESSING_BATCH_COLUMNS, type, bundle.getString(Configuration.MAX_PROCESSING_BATCH_COLUMNS+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_FILE_SIZE, type, bundle.getString(Configuration.MAX_FILE_SIZE+DESC), ModelType.INT, false, "2048"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_BUFFER_SPACE, type, bundle.getString(Configuration.MAX_BUFFER_SPACE+DESC), ModelType.INT, false, "51200"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_OPEN_FILES, type, bundle.getString(Configuration.MAX_OPEN_FILES+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
	}	

	static void getResultsetCacheDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.ENABLED, type, bundle.getString(Configuration.ENABLED+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ENTRIES, type, bundle.getString(Configuration.MAX_ENTRIES+DESC), ModelType.INT, false, "1024"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_AGE_IN_SECS, type, bundle.getString(Configuration.MAX_AGE_IN_SECS+DESC), ModelType.INT, false, "7200");//$NON-NLS-1$
		addAttribute(node, Configuration.MAX_STALENESS, type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "60");//$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_TYPE, type, bundle.getString(Configuration.CACHE_TYPE+DESC), ModelType.STRING, false, "EXPIRATION"); //$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_LOCATION, type, bundle.getString(Configuration.CACHE_LOCATION+DESC), ModelType.STRING, false, "resultset");	//$NON-NLS-1$	
	}
	
	static void getPreparedPalnCacheDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.ENABLED, type, bundle.getString(Configuration.ENABLED+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ENTRIES, type, bundle.getString(Configuration.MAX_ENTRIES+DESC), ModelType.INT, false, "512"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_AGE_IN_SECS, type, bundle.getString(Configuration.MAX_AGE_IN_SECS+DESC), ModelType.INT, false, "28800");//$NON-NLS-1$
		addAttribute(node, Configuration.MAX_STALENESS, type, bundle.getString(Configuration.MAX_STALENESS+DESC), ModelType.INT, false, "0");//$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_TYPE, type, bundle.getString(Configuration.CACHE_TYPE+DESC), ModelType.STRING, false, "LRU"); //$NON-NLS-1$
		addAttribute(node, Configuration.CACHE_LOCATION, type, bundle.getString(Configuration.CACHE_LOCATION+DESC), ModelType.STRING, false, "preparedplan");	//$NON-NLS-1$	
	}
	
	static void getSocketConfig(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.SOCKET_ENABLED, type, bundle.getString(Configuration.SOCKET_ENABLED+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_SOCKET_THREAD_SIZE, type, bundle.getString(Configuration.MAX_SOCKET_THREAD_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.IN_BUFFER_SIZE, type, bundle.getString(Configuration.IN_BUFFER_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.OUT_BUFFER_SIZE, type, bundle.getString(Configuration.OUT_BUFFER_SIZE+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.SOCKET_BINDING, type, bundle.getString(Configuration.SOCKET_BINDING+DESC), ModelType.INT, true, null);
		
		ModelNode sslNode = node.get(CHILDREN, Configuration.SSL);
		sslNode.get(TYPE).set(ModelType.OBJECT);
		sslNode.get(DESCRIPTION).set(bundle.getString(Configuration.SSL+DESC));
		sslNode.get(REQUIRED).set(false);
		sslNode.get(MAX_OCCURS).set(1);
		sslNode.get(MIN_OCCURS).set(0);
		addAttribute(node, Configuration.SSL_MODE, type, bundle.getString(Configuration.SSL_MODE+DESC), ModelType.STRING, false, "login");	//$NON-NLS-1$
		addAttribute(node, Configuration.KEY_STORE_FILE, type, bundle.getString(Configuration.KEY_STORE_FILE+DESC), ModelType.STRING, false, null);	
		addAttribute(node, Configuration.KEY_STORE_PASSWD, type, bundle.getString(Configuration.KEY_STORE_PASSWD+DESC), ModelType.STRING, false, null);
		addAttribute(node, Configuration.KEY_STORE_TYPE, type, bundle.getString(Configuration.KEY_STORE_TYPE+DESC), ModelType.STRING, false, "JKS"); //$NON-NLS-1$
		addAttribute(node, Configuration.SSL_PROTOCOL, type, bundle.getString(Configuration.SSL_PROTOCOL+DESC), ModelType.BOOLEAN, false, "SSLv3");	//$NON-NLS-1$
		addAttribute(node, Configuration.KEY_MANAGEMENT_ALG, type, bundle.getString(Configuration.KEY_MANAGEMENT_ALG+DESC), ModelType.STRING, false, "false");	//$NON-NLS-1$
		addAttribute(node, Configuration.TRUST_FILE, type, bundle.getString(Configuration.TRUST_FILE+DESC), ModelType.STRING, false, null);	
		addAttribute(node, Configuration.TRUST_PASSWD, type, bundle.getString(Configuration.TRUST_PASSWD+DESC), ModelType.STRING, false, null);	
		addAttribute(node, Configuration.AUTH_MODE, type, bundle.getString(Configuration.AUTH_MODE+DESC), ModelType.STRING, false, "anonymous");	//$NON-NLS-1$
	}
}
