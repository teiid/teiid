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



package com.metamatrix.connector.xml.base;

import java.util.Properties;

import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;

import com.metamatrix.connector.xml.SecureConnectorState;
import com.metamatrix.connector.xml.TrustedPayloadHandler;

/**
 * Encapsulates the loading of the TrustDeserializerClass for connector 
 * implementations that require security.
 *
 */
public abstract class SecureConnectorStateImpl extends XMLConnectorStateImpl implements SecureConnectorState {

	private String securityDeserializerClass;
	
	public SecureConnectorStateImpl() {
		super();
		
	}
	
	@Override
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
	
	@Override
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
			Class clazz = Thread.currentThread().getContextClassLoader().loadClass(getSecurityDeserializerClass());
			secObj = clazz.newInstance();
		} catch (Exception e) {
			String message = Messages.getString("SecureConnectorStateImpl.error.loading.trust.deserializer");
			throw new ConnectorException(e, message);
		} 
		return (TrustedPayloadHandler) secObj;
    }

}
