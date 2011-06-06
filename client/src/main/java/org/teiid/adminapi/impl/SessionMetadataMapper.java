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

import java.lang.reflect.Type;

import org.jboss.metatype.api.types.CompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.plugins.types.MutableCompositeMetaType;
import org.jboss.metatype.spi.values.MetaMapper;

public class SessionMetadataMapper extends MetaMapper<SessionMetadata> {
	private static final String SECURITY_DOMAIN = "securityDomain"; //$NON-NLS-1$
	private static final String VDB_VERSION = "VDBVersion"; //$NON-NLS-1$
	private static final String VDB_NAME = "VDBName"; //$NON-NLS-1$
	private static final String USER_NAME = "userName"; //$NON-NLS-1$
	private static final String SESSION_ID = "sessionId"; //$NON-NLS-1$
	private static final String LAST_PING_TIME = "lastPingTime"; //$NON-NLS-1$
	private static final String IP_ADDRESS = "IPAddress"; //$NON-NLS-1$
	private static final String CLIENT_HOST_NAME = "clientHostName"; //$NON-NLS-1$
	private static final String CLIENT_MAC = "clientMAC"; //$NON-NLS-1$
	private static final String CREATED_TIME = "createdTime"; //$NON-NLS-1$
	private static final String APPLICATION_NAME = "applicationName"; //$NON-NLS-1$
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(SessionMetadata.class.getName(), "The Session domain meta data"); //$NON-NLS-1$
		metaType.addItem(APPLICATION_NAME, APPLICATION_NAME, SimpleMetaType.STRING);
		metaType.addItem(CREATED_TIME, CREATED_TIME, SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem(CLIENT_HOST_NAME, CLIENT_HOST_NAME, SimpleMetaType.STRING);
		metaType.addItem(IP_ADDRESS, IP_ADDRESS, SimpleMetaType.STRING);
		metaType.addItem(LAST_PING_TIME, LAST_PING_TIME, SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem(SESSION_ID, SESSION_ID, SimpleMetaType.STRING);
		metaType.addItem(USER_NAME, USER_NAME, SimpleMetaType.STRING);
		metaType.addItem(VDB_NAME, VDB_NAME, SimpleMetaType.STRING);
		metaType.addItem(VDB_VERSION, VDB_VERSION, SimpleMetaType.INTEGER_PRIMITIVE);
		metaType.addItem(SECURITY_DOMAIN, SECURITY_DOMAIN, SimpleMetaType.STRING);
		metaType.addItem(CLIENT_MAC, CLIENT_MAC, SimpleMetaType.STRING);
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return SessionMetadata.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, SessionMetadata object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport session = new CompositeValueSupport(composite);
			
			session.set(APPLICATION_NAME, SimpleValueSupport.wrap(object.getApplicationName()));
			session.set(CREATED_TIME, SimpleValueSupport.wrap(object.getCreatedTime()));
			session.set(CLIENT_HOST_NAME, SimpleValueSupport.wrap(object.getClientHostName()));
			session.set(IP_ADDRESS, SimpleValueSupport.wrap(object.getIPAddress()));
			session.set(LAST_PING_TIME, SimpleValueSupport.wrap(object.getLastPingTime()));
			session.set(SESSION_ID, SimpleValueSupport.wrap(object.getSessionId()));
			session.set(USER_NAME, SimpleValueSupport.wrap(object.getUserName()));
			session.set(VDB_NAME,SimpleValueSupport.wrap(object.getVDBName()));
			session.set(VDB_VERSION, SimpleValueSupport.wrap(object.getVDBVersion()));
			session.set(SECURITY_DOMAIN, SimpleValueSupport.wrap(object.getSecurityDomain()));
			session.set(CLIENT_MAC, SimpleValueSupport.wrap(object.getClientHardwareAddress()));
			return session;
		}
		throw new IllegalArgumentException("Cannot convert session " + object); //$NON-NLS-1$
	}

	@Override
	public SessionMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			SessionMetadata session = new SessionMetadata();
			session.setApplicationName((String) metaValueFactory.unwrap(compositeValue.get(APPLICATION_NAME)));
			session.setCreatedTime((Long) metaValueFactory.unwrap(compositeValue.get(CREATED_TIME)));
			session.setClientHostName((String) metaValueFactory.unwrap(compositeValue.get(CLIENT_HOST_NAME)));
			session.setIPAddress((String) metaValueFactory.unwrap(compositeValue.get(IP_ADDRESS)));
			session.setLastPingTime((Long) metaValueFactory.unwrap(compositeValue.get(LAST_PING_TIME)));
			session.setSessionId((String) metaValueFactory.unwrap(compositeValue.get(SESSION_ID)));
			session.setUserName((String) metaValueFactory.unwrap(compositeValue.get(USER_NAME)));
			session.setVDBName((String) metaValueFactory.unwrap(compositeValue.get(VDB_NAME)));
			session.setVDBVersion((Integer) metaValueFactory.unwrap(compositeValue.get(VDB_VERSION)));
			session.setSecurityDomain((String) metaValueFactory.unwrap(compositeValue.get(SECURITY_DOMAIN)));
			session.setClientHardwareAddress((String) metaValueFactory.unwrap(compositeValue.get(CLIENT_MAC)));
			return session;
		}
		throw new IllegalStateException("Unable to unwrap session " + metaValue); //$NON-NLS-1$
	}

}
