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
package com.metamatrix.connector.salesforce;

import junit.framework.TestCase;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.salesforce.connection.SalesforceConnection;
import com.metamatrix.connector.salesforce.test.util.ObjectFactory;

public class TestConnector extends TestCase {

	Connector connector_not_initialized;
	Connector connector;
	Connector noCredConnector;
    
    public TestConnector() {
        super("TestConnector");
    }
	
	protected void setUp() throws Exception {
		super.setUp();
		connector_not_initialized = new Connector();
		
		ConnectorEnvironment env = ObjectFactory.getDefaultTestConnectorEnvironment();
		connector = new Connector();
		connector.start(env);
		
		ConnectorEnvironment env2 = ObjectFactory.getNoCredTestConnectorEnvironment();
		noCredConnector = new Connector();
		noCredConnector.start(env2);
	}

	public void testGetConnection() {
		ExecutionContext secContext = ObjectFactory.getDefaultSecurityContext();
		try {
			SalesforceConnection connection = (SalesforceConnection) connector.getConnection(secContext);
			assertNotNull("the connection is null", connection);
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	/*
	public void testGetConnectionTrustedToken() {
		ExecutionContext secContext = TestObjectFactory.getTokenExecutionContext();
		try {
			SalesforceConnection connection = (SalesforceConnection) noCredConnector.getConnection(secContext);
			assertNotNull("the connection is null", connection);
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	*/
	public void testGetConnectionBadUser() {
		ConnectorEnvironment env = ObjectFactory.getConnectorEnvironmentBadUser();
		ExecutionContext secContext = ObjectFactory.getDefaultSecurityContext();
		Connector localConnector = new Connector();
		try {
			localConnector.start(env);
			localConnector.getConnection(secContext);
		} catch (ConnectorException e) {
			assertFalse("There is no error message", e.getMessage().length() == 0);
			return;
		}
		fail("should have produced an exception");
	}
	
	public void testGetConnectionEmptyUser() {
		ConnectorEnvironment env = ObjectFactory.getConnectorEnvironmentEmptyUser();
		ExecutionContext secContext = ObjectFactory.getDefaultSecurityContext();
		Connector localConnector = new Connector();
		try {
			localConnector.start(env);
			localConnector.getConnection(secContext);
		} catch (ConnectorException e) {
			assertTrue("Wrong error message", e.getMessage().contains("Invalid"));
			return;
		}
		fail("should have produced an exception");
	}

	public void testGetConnectionBadPass() {
		ConnectorEnvironment env = ObjectFactory.getConnectorEnvironmentBadPass();
		ExecutionContext secContext = ObjectFactory.getDefaultSecurityContext();
		Connector localConnector = new Connector();
		try {
			localConnector.start(env);
			localConnector.getConnection(secContext);
		} catch (ConnectorException e) {
			assertFalse("There is no error message", e.getMessage().length() == 0);
			return;
		}
		fail("should have produced an exception");
	}
	
	public void testGetConnectionEmptyPass() {
		ConnectorEnvironment env = ObjectFactory.getConnectorEnvironmentEmptyPass();
		ExecutionContext secContext = ObjectFactory.getDefaultSecurityContext();
		Connector localConnector = new Connector();
		try {
			localConnector.start(env);
			localConnector.getConnection(secContext);
		} catch (ConnectorException e) {
			assertTrue("Wrong error message", e.getMessage().contains("Invalid credential configuration"));
			return;
		}
		fail("should have produced an exception");
	}
	
	public void testInitialize() {
		Connector localConnector = new Connector();
		try {
			localConnector.start(ObjectFactory.getDefaultTestConnectorEnvironment());
			assertEquals(ObjectFactory.VALID_PASSWORD, connector.getState().getPassword());
			assertEquals(ObjectFactory.VALID_USERNAME, connector.getState().getUsername());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}


	public void testGetLogger() {
		try {
			assertNotNull(connector.getLogger());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	public void testGetState() {
		assertNotNull(connector.getState());
		assertTrue(connector.getState() instanceof ConnectorState);
	}

	public void testStopNoInit() {
		connector_not_initialized.stop();
	}

}
