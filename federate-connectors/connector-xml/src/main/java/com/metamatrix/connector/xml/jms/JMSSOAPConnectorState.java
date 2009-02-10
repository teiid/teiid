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
 
 

package com.metamatrix.connector.xml.jms;

import java.util.Properties;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.SOAPConnectorState;
import com.metamatrix.connector.xml.soap.SOAPConnectorStateImpl;
import com.metamatrix.connector.xmlsource.soap.SecurityToken;

public class JMSSOAPConnectorState extends JMSXMLConnectorState implements
		SOAPConnectorState {

	public static final String AUTH_USER_PROPERTY_NAME = "AuthUserName";
	public static final String AUTH_PASSWORD_PROPERTY_NAME = "AuthPassword";
	SOAPConnectorStateImpl soapState;
	private boolean m_useBasicAuth;
	private boolean m_useWSSec;
	private String m_authUser;
	private String m_authPassword;
	public static final String AUTH_REGIME_WSSEC = "WS-Security"; //$NON-NLS-1$
	public static final String AUTH_REGIME_BASIC = "SOAP-BASIC"; //$NON-NLS-1$
	public static final String AUTH_REGIME_NONE = "None"; //$NON-NLS-1$
	public static final String AUTH_REGIME_PROPERTY_NAME = "AuthRegime"; //$NON-NLS-1$
	
	public JMSSOAPConnectorState() {
		super();
		soapState = new SOAPConnectorStateImpl();
        setUseBasicAuth(false);
        setUseWSSec(false);
        setAuthUser(new String());
        setAuthPassword(new String());   
	}

	public void setState(ConnectorEnvironment env) throws ConnectorException {
		super.setState(env);
		soapState.setState(env);
		Properties props = env.getProperties();
    	String authRegime = props.getProperty(JMSSOAPConnectorState.AUTH_REGIME_PROPERTY_NAME);
    	boolean useAuth = false;
    	if(isNotNullOrEmpty(authRegime)){
    		if(authRegime.equals(JMSSOAPConnectorState.AUTH_REGIME_NONE)){
    			setUseBasicAuth(false);
        		setUseWSSec(false);
    		} else if (authRegime.equals(JMSSOAPConnectorState.AUTH_REGIME_BASIC)) {
    			setUseBasicAuth(true);
        		setUseWSSec(false);
        		useAuth = true;
    		} else if (authRegime.equals(JMSSOAPConnectorState.AUTH_REGIME_WSSEC)) {
    			setUseBasicAuth(false);
        		setUseWSSec(true);
        		useAuth= true;
    		}
    	} else {
    		setUseBasicAuth(false);
    		setUseWSSec(false);
    	}
    	
    	if(useAuth) {
    		String user = props.getProperty(JMSSOAPConnectorState.AUTH_USER_PROPERTY_NAME);
    		if(isNotNullOrEmpty(user)) {
    			setAuthUser(user);
    		} else {
    			throw new ConnectorException(Messages.getString("SOAPConnectorStateImpl.empty.AUTH_USER_PROPERTY_NAME"));
    		}
    		
    		String pwd = props.getProperty(JMSSOAPConnectorState.AUTH_PASSWORD_PROPERTY_NAME);
    		if(isNotNullOrEmpty(pwd)) {
    			setAuthPassword(pwd);
    		} else {
    			throw new ConnectorException(Messages.getString("SOAPConnectorStateImpl.empty.AUTH_PASSWORD_PROPERTY_NAME"));
    		}
    	}
	}


	public Properties getState() {
		Properties props = super.getState(); 
		props.putAll(soapState.getState());
    	props.setProperty(JMSSOAPConnectorState.AUTH_REGIME_PROPERTY_NAME, Boolean.toString(isUseBasicAuth()));
    	props.setProperty(JMSSOAPConnectorState.AUTH_USER_PROPERTY_NAME, getAuthUser());
    	props.setProperty(JMSSOAPConnectorState.AUTH_PASSWORD_PROPERTY_NAME, getAuthPassword());

		return props;
	}
	
	public void setLogger(ConnectorLogger logger) {
		super.setLogger(logger);
		soapState.setLogger(logger);
	}


	public boolean isEncoded() {
		return soapState.isEncoded();
	}

	public boolean isRPC() {
		return soapState.isRPC();
	}

	private void setUseBasicAuth(boolean basicAuth) {
		m_useBasicAuth = basicAuth;
	}

	/* (non-Javadoc)
	 * @see com.metamatrix.connector.xml.base.SOAPConnectorState#isUseBasicAuth()
	 */
	public boolean isUseBasicAuth() {
		return m_useBasicAuth;
	}
	
    private void setUseWSSec(boolean wsSec) {
    	m_useWSSec = wsSec;
	}

	public boolean isUseWSSec() {
		return m_useWSSec;
	}

	private void setAuthUser(String authUser) {
		m_authUser = authUser;
	}


	public String getAuthUser() {
		return m_authUser;
	}

	public String getAuthPassword() {
		return m_authPassword;
	}
	
	public void setAuthPassword(String password) {
		m_authPassword = password;
	}

	public boolean isExceptionOnFault() {
		return soapState.isExceptionOnFault();
	}

	public SecurityToken getSecurityToken() {
		// TODO Auto-generated method stub
		return null;
	}

}
