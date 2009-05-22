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

package org.teiid.dqp.internal.pooling.connector;

import java.io.Serializable;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.Connector;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.api.ConnectorPropertyNames;
import org.teiid.connector.api.CredentialMap;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MappedUserIdentity;
import org.teiid.connector.basic.BasicConnector;
import org.teiid.dqp.internal.datamgr.impl.ConnectorEnvironmentImpl;
import org.teiid.dqp.internal.datamgr.impl.ExecutionContextImpl;

import com.metamatrix.common.application.ApplicationEnvironment;

/**
 */
public class TestPerUserPool extends TestCase {
	
    public TestPerUserPool(String name) {
        super(name);
    }
    
    private Connector getTestConnector(ConnectorEnvironment env) throws ConnectorException {
    	BasicConnector con = new BasicConnector() {

			@Override
			public ConnectorCapabilities getCapabilities() {
				return null;
			}

			@Override
			public Connection getConnection(ExecutionContext context)
					throws ConnectorException {
				assertTrue(context.getConnectorIdentity() instanceof MappedUserIdentity);
				return null;
			}

			@Override
			public void start(ConnectorEnvironment environment)
					throws ConnectorException {
				
			}

			@Override
			public void stop() {
				
			}
    		
    	};
    	con.setUseCredentialMap(true);
    	con.setAdminConnectionsAllowed(false);
    	con.setConnectorName("oracle system");
    	con.start(env);
    	return con;
    }
    
    public void testWrongCredentials() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        Connector factory = getTestConnector(env);
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
        connProps.put(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "oracle system"); //$NON-NLS-1$
        return new ConnectorEnvironmentImpl(connProps, Mockito.mock(ConnectorLogger.class), new ApplicationEnvironment());
    }
    
    private ExecutionContext createSecurityContext(String credentialsStr, boolean useMap, Connector factory) throws Exception {
        Serializable credentials = credentialsStr;
    	if (useMap) {
    		credentials = CredentialMap.parseCredentials(credentialsStr);
        }
    	
        // session payload
        ExecutionContextImpl impl = new ExecutionContextImpl(null, null, null, credentials, null, null, null, null, null, null);
        impl.setConnectorIdentity(factory.createIdentity(impl));
        return impl;
    }
    
    public void testCredentialMapInSessionPayload() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        Connector factory = getTestConnector(env);
        ExecutionContext ctx = createSecurityContext("(system=oracle system,user=bqt2,password=mm)", true, factory); //$NON-NLS-1$
        factory.getConnection(ctx);
    }
    
    public void testCredentialMapMissingSystem() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        Connector factory = getTestConnector(env);

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
