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
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(SessionMetadata.class.getName(), "The Session domain meta data");
		metaType.addItem("applicationName", "applicationName", SimpleMetaType.STRING);
		metaType.addItem("createdTime", "createdTime", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("clientHostName", "clientHostName", SimpleMetaType.STRING);
		metaType.addItem("IPAddress", "IPAddress", SimpleMetaType.STRING);
		metaType.addItem("lastPingTime", "lastPingTime", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("sessionId", "sessionId", SimpleMetaType.LONG_PRIMITIVE);
		metaType.addItem("userName", "userName", SimpleMetaType.STRING);
		metaType.addItem("VDBName", "VDBName", SimpleMetaType.STRING);
		metaType.addItem("VDBVersion", "VDBVersion", SimpleMetaType.INTEGER_PRIMITIVE);
		metaType.addItem("securityDomain", "SecurityDomain", SimpleMetaType.STRING);
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
			
			session.set("applicationName", SimpleValueSupport.wrap(object.getName()));
			session.set("createdTime", SimpleValueSupport.wrap(object.getCreatedTime()));
			session.set("clientHostName", SimpleValueSupport.wrap(object.getClientHostName()));
			session.set("IPAddress", SimpleValueSupport.wrap(object.getIPAddress()));
			session.set("lastPingTime", SimpleValueSupport.wrap(object.getLastPingTime()));
			session.set("sessionId", SimpleValueSupport.wrap(object.getSessionId()));
			session.set("userName", SimpleValueSupport.wrap(object.getUserName()));
			session.set("VDBName",SimpleValueSupport.wrap(object.getVDBName()));
			session.set("VDBVersion", SimpleValueSupport.wrap(object.getVDBVersion()));
			session.set("securityDomain", SimpleValueSupport.wrap(object.getSecurityDomain()));
			
			return session;
		}
		throw new IllegalArgumentException("Cannot convert session " + object);
	}

	@Override
	public SessionMetadata unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			SessionMetadata session = new SessionMetadata();
			session.setApplicationName((String) metaValueFactory.unwrap(compositeValue.get("applicationName")));
			session.setCreatedTime((Long) metaValueFactory.unwrap(compositeValue.get("createdTime")));
			session.setClientHostName((String) metaValueFactory.unwrap(compositeValue.get("clientHostName")));
			session.setIPAddress((String) metaValueFactory.unwrap(compositeValue.get("IPAddress")));
			session.setLastPingTime((Long) metaValueFactory.unwrap(compositeValue.get("lastPingTime")));
			session.setSessionId((Long) metaValueFactory.unwrap(compositeValue.get("sessionId")));
			session.setUserName((String) metaValueFactory.unwrap(compositeValue.get("userName")));
			session.setVDBName((String) metaValueFactory.unwrap(compositeValue.get("VDBName")));
			session.setVDBVersion((Integer) metaValueFactory.unwrap(compositeValue.get("VDBVersion")));
			session.setSecurityDomain((String) metaValueFactory.unwrap(compositeValue.get("securityDomain")));
			return session;
		}
		throw new IllegalStateException("Unable to unwrap session " + metaValue);
	}

}
