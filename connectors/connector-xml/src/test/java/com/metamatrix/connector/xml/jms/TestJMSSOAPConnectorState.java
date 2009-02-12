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

package com.metamatrix.connector.xml.jms;

import java.util.Properties;

import javax.jms.DeliveryMode;
import javax.jms.Session;

import junit.framework.TestCase;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.api.SysLogger;
import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.xml.base.ProxyObjectFactory;
import com.metamatrix.connector.xml.soap.SOAPConnectorStateImpl;

public class TestJMSSOAPConnectorState extends TestCase {

	JMSSOAPConnectorState state;
	
	protected void setUp() throws Exception {
		super.setUp();
		
		state = new JMSSOAPConnectorState();
		state.setLogger(new SysLogger(false));
		state.setState(getEnv(ProxyObjectFactory.getSOAPJMSPropertiesAuth()));
	}
	
	protected void tearDown() throws Exception {
		super.tearDown();
	}

	@Override
	protected void finalize() throws Throwable {
		super.finalize();
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.setState(Properties)'
	 */
	public void testSetState() {
		JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
		try {
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(ProxyObjectFactory.getSOAPJMSPropertiesAuth()));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.JMSConnectorState()'
	 */
	public void testJMSConnectorState() {
		JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
		try {
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(ProxyObjectFactory.getSOAPJMSPropertiesAuth()));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.JMSConnectorState(Properties)'
	 */
	public void testJMSConnectorStateProperties() {
		try {
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(ProxyObjectFactory.getSOAPJMSPropertiesAuth()));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testBadInitialContext() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.INITIAL_CONTEXT_FACTORY, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			this.assertTrue(true);
			return;
		}
		fail("This should have thrown a ConnectorException");
	}
	
	public void testBadProviderURL() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.PROVIDER_URL, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			this.assertTrue(true);
			return;
		}
		fail("This should have thrown a ConnectorException");
	}
	
	public void testBadConnectionFactory() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.CONNECTION_FACTORY, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			this.assertTrue(true);
			return;
		}
		fail("This should have thrown a ConnectorException");
	}
	
	public void testEmptyOutboundJMSDestination() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.OUTBOUND_JMS_DESTINATION, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getLocalizedMessage());
			return;
		}
	}
	
	public void testEmptyInboundJMSDestination() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.INBOUND_JMS_DESTINATION, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getLocalizedMessage());
			return;
		}
	}
	
	public void testEmptyJMSDestinations() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.INBOUND_JMS_DESTINATION, "");
			props.setProperty(JMSConnectorState.OUTBOUND_JMS_DESTINATION, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			assertTrue(true);
			return;
		}
		fail("This should have thrown an exception about having no destinations");
	}
	
	public void testEmptyUsername() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesNoAuth();
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getLocalizedMessage());
			return;
		}
	}

	public void testEmptyPassword() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesNoAuth();
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getLocalizedMessage());
			return;
		}
	}
	
	public void testNullAcknowledgementMode() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.ACKNOWLEDGEMENT_MODE, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for empty ACK Mode");
	}
	
	public void testEmptyCorrelationID() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.CORRELATION_ID, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testEmptyDeliveryMode() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.MESSAGE_DELIVERY_MODE, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("This should have thrown an exception for an empty delivery mode");
	}
	
	public void testEmptyReplyTo() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.REPLY_TO_DESTINATION, "");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testStringReceiveTimeout() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.RECEIVE_TIMEOUT, "not a number");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for a Number Format");
	}
	
	public void testStringRetryCount() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.CONNECTION_RETRY_COUNT, "lots");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for a Number Format");
	}
	
	public void testStringMessagePriority() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.MESSAGE_PRIORITY, "high");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for a Number Format");
	}
	
	public void testStringMessageDuration() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.MESSAGE_DURATION, "really long");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for a Number Format");
	}
	
	public void testMessagePriorityOutOfRange() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(JMSConnectorState.MESSAGE_PRIORITY, "16");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			return;
		}
		fail("Should have thrown a ConnectorException for Message Priority out of range");
	}
	
	public void testDocLiteral() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.DOC_LITERAL_STYLE);
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
			assertFalse("Should not be encoded", testState.isEncoded());
			assertFalse("Should not be RPC", testState.isRPC());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testDocEncoded() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.DOC_ENCODED_STYLE);
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
			assertTrue("Should be encoded", testState.isEncoded());
			assertFalse("Should not be RPC", testState.isRPC());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testRPCEncoded() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.RPC_ENC_STYLE);
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
			assertTrue("Should be encoded", testState.isEncoded());
			assertTrue("Should be RPC", testState.isRPC());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testRPCLiteral() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, SOAPConnectorStateImpl.RPC_LITERAL_STYLE);
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
			assertFalse("Should not be encoded", testState.isEncoded());
			assertTrue("Should be RPC", testState.isRPC());
		} catch (ConnectorException e) {
			fail(e.getMessage());
		}
	}
	
	public void testBogusEncoding() {
		try {
			Properties props = ProxyObjectFactory.getSOAPJMSPropertiesAuth();
			props.setProperty(SOAPConnectorStateImpl.ENCODING_STYLE_PROPERTY_NAME, "Shizno");
			JMSSOAPConnectorState testState = new JMSSOAPConnectorState();
			testState.setLogger(new SysLogger(false));
			testState.setState(getEnv(props));
		} catch (ConnectorException e) {
			assertTrue(e.getMessage().indexOf("Encoding Style")!= -1);
			return;
		}
		fail("This should have thrown an exception");
	}
	
	public void testGetState() {
		assertTrue(state.getState() instanceof Properties );
	}
	
	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.getInitialContextFactoryName()'
	 */
	public void testGetInitialContextFactoryName() {
		assertEquals("These values should be equal", ProxyObjectFactory.INITIAL_CONTEXT_FACTORY, state.getInitialContextFactoryName());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.getPrimaryProviderUrl()'
	 */
	public void testGetPrimaryProviderUrl() {
		assertEquals("These values should be equal", "tcp://localhost:3035/", state.getPrimaryProviderUrl());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.getConnectionFactoryName()'
	 */
	public void testGetConnectionFactoryName() {
		assertEquals("These values should be equal", "ConnectionFactory", state.getConnectionFactoryName());
	}
	
	public void testGetOutboundJMSDestination() {
		assertEquals("These values should be equal", ProxyObjectFactory.JMS_DESTINATION, state.getOutboundJMSDestination());
	}
	
	public void testGetInboundJMSDestination() {
		assertEquals("These values should be equal", ProxyObjectFactory.JMS_DESTINATION, state.getInboundJMSDestination());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.getUserName()'
	 */
	public void testGetUserName() {
		assertEquals("These values should be equal", "user", state.getUserName());
	}

	/*
	 * Test method for 'com.metamatrix.connector.xml.jms.JMSXMLConnectorState.getPassword()'
	 */
	public void testGetPassword() {
		assertEquals("These values should be equal", "1234", state.getPassword());
	}

	public void testGetAckMode() {
		assertEquals("These values should be equal", Session.CLIENT_ACKNOWLEDGE, state.getAcknowledgementMode());
	}
	
	public void testGetReceiveTimeout() {
		assertEquals("These values should be equal", Integer.parseInt("5000"), state.getReceiveTimeout());
	}
	
	public void testGetCorrelationIdPrefix() {
		assertEquals("These values should be equal", "prefix" , state.getCorrelationIdPrefix());
	}
	
	public void testGetMessagePriority() {
		assertEquals("These values should be equal", 7 , state.getMesssagePriority());
	}

	public void testGetMessageDuration() {
		assertEquals("These values should be equal", 500 , state.getMessageDuration());
	}
	
	public void testGetMessageDeliveryMode() {
		assertEquals("These values should be equal", DeliveryMode.PERSISTENT , state.getMessageDeliveryMode());
	}
	
	public void testGetReplyToDestination() {
		assertEquals("These values should be equal", "reply" , state.getReplyToDestination());
	}
	
	public void testGetConnectionRetryCount() {
		assertEquals("These values should be equal", 1 , state.getConnectionRetryCount());
	}
	
	public void testGetAuthPassword() {
		assertEquals("These values should be equal", "getSome" , state.getAuthPassword());
	}
	
	public void testGetAuthUser() {
		assertEquals("These values should be equal", "someUser" , state.getAuthUser());
	}
	
	public void testIsEncoded() {
		assertEquals("These values should be equal", Boolean.TRUE.booleanValue() , state.isEncoded());
	}
	
	public void testIsRPC() {
		assertEquals("These values should be equal", Boolean.TRUE.booleanValue() , state.isRPC());
	}
	
	public void testIsUseBasicAuth() {
		assertEquals("These values should be equal", Boolean.TRUE.booleanValue() , state.isUseBasicAuth());
	}
	
	public void testIsExceptionOnFault() {
		assertEquals("These values should be equal", Boolean.TRUE.booleanValue() , state.isExceptionOnFault());
	}
    
	private ConnectorEnvironment getEnv(Properties props) {
    	return EnvironmentUtility.createEnvironment(props);
    }
}
