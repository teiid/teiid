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

package com.metamatrix.connector.jdbc;

import java.sql.Driver;
import java.util.Properties;

import com.metamatrix.connector.api.Connection;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.pool.CredentialMap;
import com.metamatrix.connector.pool.UserIdentityFactory;

/**
 */
public class JDBCUserIdentityConnectionFactory extends JDBCSourceConnectionFactory {

    private Driver driver;
    private String url;
    private int transIsoLevel;
    private String system;

    public JDBCUserIdentityConnectionFactory() {
        super(new UserIdentityFactory());
    }
    
    public void initialize(ConnectorEnvironment env) throws ConnectorException {
        super.initialize(env);
        system = getConnectorEnvironment().getConnectorName();
        verifyConnectionProperties(env.getProperties());
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

    public Connection getConnection(ExecutionContext context) throws ConnectorException {
        String[] userProperties = getUserProperties(context);
        
        Properties props = new Properties();
        props.put("user", userProperties[0]); //$NON-NLS-1$
        props.put("password", userProperties[1]);       //$NON-NLS-1$
        return createJDBCConnection(this.driver, getUrl(), getTransactionIsolationLevel(), props);
    }
    
    protected String[] getUserProperties(ExecutionContext context) throws ConnectorException {
        
        // By default, assume the session token is a CredentialMap and pull out the user/password props
        Object trustedPayload = context.getTrustedPayload(); 
        
		if(trustedPayload instanceof CredentialMap) {
			CredentialMap credentials = (CredentialMap)trustedPayload;
			String user = credentials.getUser(system);
	        validatePropertyExists(user, CredentialMap.USER_KEYWORD);
	                
	        String password = credentials.getPassword(system);
	        validatePropertyExists(password, CredentialMap.PASSWORD_KEYWORD);        
	         
	        return new String[] { user, password };
        }

        throw new ConnectorException(JDBCPlugin.Util.getString("JDBCUserIdentityConnectionFactory.Unable_to_get_credentials")); //$NON-NLS-1$
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
