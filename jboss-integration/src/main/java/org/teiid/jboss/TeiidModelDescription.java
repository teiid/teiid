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
import static org.teiid.jboss.Configuration.DESC;
import static org.teiid.jboss.Configuration.addAttribute;

import java.util.ResourceBundle;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

class TeiidModelDescription {
			
	static void getQueryEngineDescription(ModelNode node, String type, ResourceBundle bundle) {
		addAttribute(node, Configuration.ENGINE_NAME, type, bundle.getString(Configuration.ENGINE_NAME+Configuration.DESC), ModelType.STRING, true, null);		
		addAttribute(node, Configuration.MAX_THREADS, type, bundle.getString(Configuration.MAX_THREADS+DESC), ModelType.INT, false, "64"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ACTIVE_PLANS, type, bundle.getString(Configuration.MAX_ACTIVE_PLANS+DESC), ModelType.INT, false, "20"); //$NON-NLS-1$
		addAttribute(node, Configuration.USER_REQUEST_SOURCE_CONCURRENCY, type, bundle.getString(Configuration.USER_REQUEST_SOURCE_CONCURRENCY+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		addAttribute(node, Configuration.TIME_SLICE_IN_MILLI, type, bundle.getString(Configuration.TIME_SLICE_IN_MILLI+DESC), ModelType.INT, false, "2000"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_ROWS_FETCH_SIZE, type, bundle.getString(Configuration.MAX_ROWS_FETCH_SIZE+DESC), ModelType.INT, false, "20480"); //$NON-NLS-1$
		addAttribute(node, Configuration.LOB_CHUNK_SIZE_IN_KB, type, bundle.getString(Configuration.LOB_CHUNK_SIZE_IN_KB+DESC), ModelType.INT, false, "100"); //$NON-NLS-1$
		addAttribute(node, Configuration.QUERY_THRESHOLD_IN_SECS, type, bundle.getString(Configuration.QUERY_THRESHOLD_IN_SECS+DESC), ModelType.INT, false, "600"); //$NON-NLS-1$		
		addAttribute(node, Configuration.MAX_SOURCE_ROWS, type, bundle.getString(Configuration.MAX_SOURCE_ROWS+DESC), ModelType.INT, false, "-1"); //$NON-NLS-1$
		addAttribute(node, Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS, type, bundle.getString(Configuration.EXCEPTION_ON_MAX_SOURCE_ROWS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		addAttribute(node, Configuration.MAX_ODBC_LOB_SIZE_ALLOWED, type, bundle.getString(Configuration.MAX_ODBC_LOB_SIZE_ALLOWED+DESC), ModelType.INT, false, "5242880"); //$NON-NLS-1$
		addAttribute(node, Configuration.EVENT_DISTRIBUTOR_NAME, type, bundle.getString(Configuration.EVENT_DISTRIBUTOR_NAME+DESC), ModelType.STRING, false, "teiid/event-distributor"); //$NON-NLS-1$
		addAttribute(node, Configuration.DETECTING_CHANGE_EVENTS, type, bundle.getString(Configuration.DETECTING_CHANGE_EVENTS+DESC), ModelType.BOOLEAN, false, "true"); //$NON-NLS-1$
		
		//session stuff
		addAttribute(node, Configuration.SECURITY_DOMAIN, type, bundle.getString(Configuration.SECURITY_DOMAIN+DESC), ModelType.STRING, false, null);
		addAttribute(node, Configuration.MAX_SESSIONS_ALLOWED, type, bundle.getString(Configuration.MAX_SESSIONS_ALLOWED+DESC), ModelType.INT, false, "5000"); //$NON-NLS-1$
		addAttribute(node, Configuration.SESSION_EXPIRATION_TIME_LIMIT, type, bundle.getString(Configuration.SESSION_EXPIRATION_TIME_LIMIT+DESC), ModelType.INT, false, "0"); //$NON-NLS-1$
		
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
	
	
	private static void getSocketConfig(ModelNode node, String type, ResourceBundle bundle) {
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
