/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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



package com.metamatrix.connector.xml.base;

import java.util.Properties;

import com.metamatrix.connector.xml.SecureConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;

/**
 * Encapsulates the loading of the TrustDeserializerClass for connector 
 * implementations that require security.
 * @author Jdoyle
 *
 */
public abstract class SecureConnectorStateImpl extends XMLConnectorStateImpl implements SecureConnectorState {

	private String securityDeserializerClass;
	
	public SecureConnectorStateImpl() {
		super();
		
	}
	
	public void setState(ConnectorEnvironment env) throws ConnectorException {
		super.setState(env);
		
        String secure = env.getProperties().getProperty(SECURITY_DESERIALIZER_CLASS);
        if(secure != null && !secure.equals("")) {
        	setSecurityDeserializerClass(secure);
        } else {
        	throw new ConnectorException(
        		Messages.getString("SecureConnectorStateImpl.empty.trust.deserializer"));
        }
        
        //ensure that we can get it when we need it.
        getTrustDeserializerInstance();
	}
	
	public Properties getState() {
		Properties props = super.getState();
        props.setProperty(SECURITY_DESERIALIZER_CLASS, getSecurityDeserializerClass());
		return props;
	}
	
    private void setSecurityDeserializerClass(String secClass) {
    	securityDeserializerClass = secClass;
    }
    
    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SecureConnectorState#getSecurityDeserializerClass()
	 */
    public String getSecurityDeserializerClass() {
    	return securityDeserializerClass;
    }

    /* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SecureConnectorState#getTrustDeserializerInstance()
	 */
    public TrustedPayloadHandler getTrustDeserializerInstance() throws ConnectorException {
    	Object secObj;
		try {
			secObj = Class.forName(getSecurityDeserializerClass()).newInstance();
		} catch (Exception e) {
			String message = Messages.getString("SecureConnectorStateImpl.error.loading.trust.deserializer");
			throw new ConnectorException(e, message);
		} 
		return (TrustedPayloadHandler) secObj;
    }

}
