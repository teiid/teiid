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

package com.metamatrix.connector.jdbc;

import java.sql.Driver;
import java.util.Properties;

import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.pool.ConnectorIdentity;
import com.metamatrix.data.pool.CredentialMap;
import com.metamatrix.data.pool.SourceConnection;
import com.metamatrix.data.pool.UserIdentity;

/**
 */
public class JDBCUserIdentityConnectionFactory extends JDBCSourceConnectionFactory {

    private Driver driver;
    private String url;
    private int transIsoLevel;
    private String system;

    public JDBCUserIdentityConnectionFactory() {
        super();
    }
    
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        system = getConnectorEnvironment().getConnectorName();
        verifyConnectionProperties(env.getProperties());
    }

    public boolean isSingleIdentity() {
        return false;
    }
    
    protected void verifyConnectionProperties(Properties connectionProps) throws ConnectorException{
        // Find driver 
        String driverClassName = connectionProps.getProperty(JDBCPropertyNames.DRIVER_CLASS);
        driver = createDriver(driverClassName);
        
        // check URL if there is one
        url = connectionProps.getProperty(JDBCPropertyNames.URL);
        if(url != null && url.trim().length() > 0) {
            validateURL(driver, url);
        }
        
        // Get transaction isolation level
        transIsoLevel = interpretTransactionIsolationLevel( connectionProps.getProperty(JDBCPropertyNames.TRANSACTION_ISOLATION_LEVEL));        
    }
    
    protected String getUrl() {
        return this.url;
    }
    
    protected int getTransactionIsolationLevel() {
        return this.transIsoLevel;
    }

    public ConnectorIdentity createIdentity(SecurityContext context) throws ConnectorException {
        return new UserIdentity(context);
    }

    public SourceConnection createConnection(ConnectorIdentity id) throws ConnectorException {
        SecurityContext context = id.getSecurityContext();  
        String[] userProperties = getUserProperties(context);
        
        Properties props = new Properties();
        props.put("user", userProperties[0]); //$NON-NLS-1$
        props.put("password", userProperties[1]);       //$NON-NLS-1$
        return createJDBCConnection(this.driver, getUrl(), getTransactionIsolationLevel(), props);
    }
    
    protected String[] getUserProperties(SecurityContext context) throws ConnectorException {
        // First, check the command payload for a CredentialMap - if not found, then check 
        // the trusted payload for a CredentialMap.  If neither is a CredentialMap, then throw
        // an exception - someone needs to subclass and override this method for their particular
        // credential lookup strategy.
        
        Object commandPayload = context.getExecutionPayload();
        if(commandPayload instanceof CredentialMap) {
            return extractPayload((CredentialMap)commandPayload);
        }
        
        // By default, assume the session token is a CredentialMap and pull out the user/password props
        Object trustedPayload = context.getTrustedPayload(); 
        
		if(trustedPayload instanceof CredentialMap) {
            return extractPayload((CredentialMap)trustedPayload);
        }

        throw new ConnectorException(JDBCPlugin.Util.getString("JDBCUserIdentityConnectionFactory.Unable_to_get_credentials")); //$NON-NLS-1$
    }   

    /** 
     * @param credentialMap
     * @return
     * @since 4.3
     */
    private String[] extractPayload(CredentialMap credentials) throws ConnectorException {
        String user = credentials.getUser(system);
        validatePropertyExists(user, CredentialMap.USER_KEYWORD);
                
        String password = credentials.getPassword(system);
        validatePropertyExists(password, CredentialMap.PASSWORD_KEYWORD);        
         
        return new String[] { user, password };
    }

    /** 
     * @param property
     * @throws ConnectorException
     * @since 4.3
     */
    private void validatePropertyExists(String property, String propertyName) throws ConnectorException {
        if(property == null) {
            throw new ConnectorException(JDBCPlugin.Util.getString("JDBCUserIdentityConnectionFactory.Connection_property_missing", propertyName, system)); //$NON-NLS-1$
        }
    }
   
}
