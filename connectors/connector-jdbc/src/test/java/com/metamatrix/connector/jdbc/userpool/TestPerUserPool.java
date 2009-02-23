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

package com.metamatrix.connector.jdbc.userpool;

import java.io.Serializable;
import java.util.Properties;

import javax.sql.DataSource;

import org.mockito.Mockito;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.CredentialMap;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.internal.ConnectorPropertyNames;
import com.metamatrix.connector.jdbc.JDBCConnector;
import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.dqp.internal.datamgr.impl.ExecutionContextImpl;

/**
 */
public class TestPerUserPool extends TestCase {
	
    private static String TEST_URL = "jdbc:mmx:oracle://slntds04:1521;SID=ds04"; //$NON-NLS-1$

    public TestPerUserPool(String name) {
        super(name);
    }
    
    public void testWrongCredentials() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        JDBCConnector factory = new JDBCConnector();
        factory.setUseCredentialMap(true);
        factory.setAdminConnectionsAllowed(false);
        factory.start(env);
        try {
            ExecutionContext ctx = createSecurityContext("pw1", false, factory); //$NON-NLS-1$
			factory.getConnection(ctx);
			fail("expected failure"); //$NON-NLS-1$
		} catch (ConnectorException e) {
			assertEquals("Unable to extract credentials from command payload or trusted session payload for per-user connection.", e.getMessage()); //$NON-NLS-1$
		}
    }
    
    private ConnectorEnvironment initConnectorEnvironment() throws Exception {
        final Properties connProps = new Properties();
        connProps.put(JDBCPropertyNames.CONNECTION_SOURCE_CLASS, "com.metamatrix.jdbc.oracle.OracleDriver"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.URL, TEST_URL); 
        connProps.put(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "oracle system"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.EXT_CAPABILITY_CLASS, "com.metamatrix.connector.jdbc.oracle.OracleCapabilities"); //$NON-NLS-1$
        return EnvironmentUtility.createEnvironment(connProps, false);
    }
    
    private ExecutionContext createSecurityContext(String credentialsStr, boolean useMap, JDBCConnector factory) throws Exception {
        Serializable credentials = credentialsStr;
    	if (useMap) {
    		credentials = CredentialMap.parseCredentials(credentialsStr);
        }
    	
        // session payload
        ExecutionContextImpl impl = new ExecutionContextImpl(null, null, null, credentials, null, null, null, null, null, null, false);
        impl.setConnectorIdentity(factory.createIdentity(impl));
        return impl;
    }
    
    public void testCredentialMapInSessionPayload() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        JDBCConnector factory = new JDBCConnector() {
        	@Override
        	public DataSource getDataSource() {
        		return Mockito.mock(DataSource.class);
        	}
        };
        factory.setUseCredentialMap(true);
        factory.setAdminConnectionsAllowed(false);
        factory.setConnectorName("oracle system");
        factory.start(env);
        
        ExecutionContext ctx = createSecurityContext("(system=oracle system,user=bqt2,password=mm)", true, factory); //$NON-NLS-1$
        factory.getConnection(ctx);
    }
    
    public void testCredentialMapMissingSystem() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        JDBCConnector factory = new JDBCConnector();
        factory.setUseCredentialMap(true);
        factory.setAdminConnectionsAllowed(false);
        factory.setConnectorName("oracle system");
        factory.start(env);

        // Set system to "x" instead of "oracle system" which will cause no credentials to be found
        try {
            ExecutionContext ctx = createSecurityContext("(system=x,user=bqt2,password=mm)", true, factory); //$NON-NLS-1$
            factory.getConnection(ctx);
            fail("Expected exception when creating connection with missing system credentials"); //$NON-NLS-1$
        } catch(Exception e) {
            // expected
            assertEquals("Payload missing credentials for oracle system", e.getMessage()); //$NON-NLS-1$
        }
    }

}
