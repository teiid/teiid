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
package org.teiid.adminapi.impl;

import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;
 

public class SessionMetadataMapper {
	private static final String SECURITY_DOMAIN = "securityDomain"; //$NON-NLS-1$
	private static final String VDB_VERSION = "VDBVersion"; //$NON-NLS-1$
	private static final String VDB_NAME = "VDBName"; //$NON-NLS-1$
	private static final String USER_NAME = "userName"; //$NON-NLS-1$
	private static final String SESSION_ID = "sessionId"; //$NON-NLS-1$
	private static final String LAST_PING_TIME = "lastPingTime"; //$NON-NLS-1$
	private static final String IP_ADDRESS = "IPAddress"; //$NON-NLS-1$
	private static final String CLIENT_HOST_NAME = "clientHostName"; //$NON-NLS-1$
	private static final String CREATED_TIME = "createdTime"; //$NON-NLS-1$
	private static final String APPLICATION_NAME = "applicationName"; //$NON-NLS-1$
	
	
	public static ModelNode wrap(SessionMetadata object) {
		if (object == null) {
			return null;
		}
		ModelNode session = new ModelNode();
		session.get(ModelNodeConstants.TYPE).set(ModelType.OBJECT);
			
		session.get(APPLICATION_NAME).set(object.getApplicationName());
		session.get(CREATED_TIME).set(object.getCreatedTime());
		session.get(CLIENT_HOST_NAME).set(object.getClientHostName());
		session.get(IP_ADDRESS).set(object.getIPAddress());
		session.get(LAST_PING_TIME).set(object.getLastPingTime());
		session.get(SESSION_ID).set(object.getSessionId());
		session.get(USER_NAME).set(object.getUserName());
		session.get(VDB_NAME).set(object.getVDBName());
		session.get(VDB_VERSION).set(object.getVDBVersion());
		session.get(SECURITY_DOMAIN).set(object.getSecurityDomain());
			
		return session;
	}

	public static SessionMetadata unwrap(ModelNode node) {
		if (node == null)
			return null;
			
		SessionMetadata session = new SessionMetadata();
		session.setApplicationName(node.get(APPLICATION_NAME).asString());
		session.setCreatedTime(node.get(CREATED_TIME).asLong());
		session.setClientHostName(node.get(CLIENT_HOST_NAME).asString());
		session.setIPAddress(node.get(IP_ADDRESS).asString());
		session.setLastPingTime(node.get(LAST_PING_TIME).asLong());
		session.setSessionId(node.get(SESSION_ID).asString());
		session.setUserName(node.get(USER_NAME).asString());
		session.setVDBName(node.get(VDB_NAME).asString());
		session.setVDBVersion(node.get(VDB_VERSION).asInt());
		session.setSecurityDomain(node.get(SECURITY_DOMAIN).asString());
		return session;
	}

}
