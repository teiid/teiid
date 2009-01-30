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

package com.metamatrix.connector.jdbc.userpool;

import java.io.Serializable;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.connector.jdbc.JDBCPropertyNames;
import com.metamatrix.connector.jdbc.JDBCSourceConnectionFactory;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.api.ConnectorLogger;
import com.metamatrix.data.api.SecurityContext;
import com.metamatrix.data.api.TypeFacility;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.ILanguageFactory;
import com.metamatrix.data.pool.CredentialMap;
import com.metamatrix.dqp.internal.datamgr.ConnectorPropertyNames;
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
        JDBCSourceConnectionFactory factory = new MockExampleConnectionFactory();
        factory.initialize(env);
        SecurityContext ctx = createSecurityContext("pw1", false, factory); //$NON-NLS-1$
        try {
			factory.getConnection(ctx);
			fail("expected failure"); //$NON-NLS-1$
		} catch (ConnectorException e) {
			assertEquals("Unable to extract credentials from command payload or trusted session payload for per-user connection.", e.getMessage()); //$NON-NLS-1$
		}
    }
    
    private ConnectorEnvironment initConnectorEnvironment() throws Exception {
        final Properties connProps = new Properties();
        connProps.put(JDBCPropertyNames.DRIVER_CLASS, "com.metamatrix.jdbc.oracle.OracleDriver"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.URL, TEST_URL); 
        connProps.put(ConnectorPropertyNames.CONNECTOR_BINDING_NAME, "oracle system"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.EXT_CAPABILITY_CLASS, "com.metamatrix.connector.jdbc.oracle.OracleCapabilities"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.EXT_CONNECTION_FACTORY_CLASS, "com.metamatrix.connector.jdbc.oracle.OracleUserIdentityConnectionFactory"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.EXT_SQL_TRANSLATOR_CLASS, "com.metamatrix.connector.jdbc.oracle.OracleSQLTranslator"); //$NON-NLS-1$
        connProps.put(JDBCPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS, "com.metamatrix.connector.jdbc.oracle.OracleResultsTranslator"); //$NON-NLS-1$
        ConnectorEnvironment env = new ConnectorEnvironment() {

			public String getConnectorName() {
				return "oracle system"; //$NON-NLS-1$
			}

			public ILanguageFactory getLanguageFactory() {
				return null;
			}

			public ConnectorLogger getLogger() {
				return null;
			}

			public Properties getProperties() {
				return connProps;
			}

			public TypeFacility getTypeFacility() {
				return null;
			}
        	
        };
        return env;
    }
    
    private SecurityContext createSecurityContext(String credentialsStr, boolean useMap, JDBCSourceConnectionFactory factory) throws Exception {
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
        JDBCSourceConnectionFactory factory = new MockExampleConnectionFactory();
        factory.initialize(env);
        
        SecurityContext ctx = createSecurityContext("(system=oracle system,user=bqt2,password=mm)", true, factory); //$NON-NLS-1$
        MockSourceConnection conn = (MockSourceConnection)factory.getConnection(ctx);
        assertEquals("bqt2", conn.getUser()); //$NON-NLS-1$
        assertEquals("mm", conn.getPassword()); //$NON-NLS-1$
    }
    
    public void testCredentialMapMissingSystem() throws Exception {
        ConnectorEnvironment env = initConnectorEnvironment();
        JDBCSourceConnectionFactory factory = new MockExampleConnectionFactory();
        factory.initialize(env);

        // Set system to "x" instead of "oracle system" which will cause no credentials to be found
        SecurityContext ctx = createSecurityContext("(system=x,user=bqt2,password=mm)", true, factory); //$NON-NLS-1$
        try {
            factory.getConnection(ctx);
            fail("Expected exception when creating connection with missing system credentials"); //$NON-NLS-1$
        } catch(Exception e) {
            // expected
            assertEquals("Required connection property \"user\" missing for system \"oracle system\".", e.getMessage()); //$NON-NLS-1$
        }
    }

}
