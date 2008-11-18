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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.client;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;
import java.util.Timer;

import org.mockito.Mockito;

import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.common.util.NetUtils;
import com.metamatrix.jdbc.api.ConnectionProperties;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.util.ProductInfoConstants;

import junit.framework.TestCase;

/**
 * <code>TestCase</case> for <code>SocketServerConnection</code>
 * @see SocketServerConnection
 * @since Westport
 */
public class TestSocketServerConnection extends TestCase {

	/**
	 * Validate that the user name property in the connection properties object is 
	 * updated with the user name from <code>LogonResult</code> after a 
	 * <code>SocketServerConnection</code> is established. 
	 * 
	 * <p>The expected results contains a fully-qualified user 
	 * name while the initial connection properties uses the 
	 * unqualified version of the user name.</p>
	 * 
	 * <p>This test case simulates what occurs when a client 
	 * establishes a connection with a server and the user name 
	 * provided at connection time is the unqualified user name. In 
	 * such cases the server will attempt to resolve the 
	 * unqualified user name to a membership domain and then use the 
	 * fully-qualified user name in the session cache. The server 
	 * also returns this fully-qualified user name in the 
	 * <code>LogonResult</code> which is returned to <code>SocketServerConnection</code></p>
	 * @throws Throwable 
	 *  
	 * @since Westport    
	 */
	public void testSocketServerConnection_PropertiesUserName() throws Throwable {
		// an unqualified user name
		String userName = "tuser"; //$NON-NLS-1$
		// the user's membership domain name
		String membershipDomainName = "test.domain"; //$NON-NLS-1$

		// for our pre connection properties
		Properties p = new Properties();
		// put the unqualified user name in our connection properties
		p.setProperty(MMURL_Properties.JDBC.USER_NAME, userName);
		
		// for our post connection properties
		Properties e = new Properties();
		// put the unqualified user name in our connection properties
		e.setProperty(MMURL_Properties.JDBC.USER_NAME, userName + "@" + membershipDomainName); //$NON-NLS-1$
       
		LogonResult lr = this.createLogonResult(null, userName + "@" + membershipDomainName, null, 0); //$NON-NLS-1$
		this.doTestSocketServerConnectionProperties(p, e, lr, true);
	}

	/**
	 * Validate that the client host name and IP address property in 
	 * the connection properties object is set after a <code>SocketServerConnection</code> 
	 * is established. 
	 * 
	 * <p>The expected results contains the host name and IP address 
	 * of the local machine as returned by <code>NetUtils</code>. 
	 * These values are not put into the initial connection object 
	 * and it is up to <code>SocketServerConnection</code> to place 
	 * the values into the connection properties object during the 
	 * connection process.</p>
	 * @throws Throwable 
	 *  
	 * @since Westport    
	 */
	public void testSocketServerConnection_PropertiesClientHost() throws Throwable {
		// for our pre connection properties
		Properties p = new Properties();

		// for our post connection properties
		Properties e = new Properties();
		// the host name and IP address are expected
        try {
        	e.setProperty(ProductInfoConstants.CLIENT_IP_ADDRESS, NetUtils.getHostAddress());
        } catch (UnknownHostException err1) {
        	e.setProperty(ProductInfoConstants.CLIENT_IP_ADDRESS, "UnknownClientAddress"); //$NON-NLS-1$
        }
        try {
        	e.setProperty(ProductInfoConstants.CLIENT_HOSTNAME, NetUtils.getHostname());
        } catch (UnknownHostException err1) {
        	e.setProperty(ProductInfoConstants.CLIENT_HOSTNAME, "UnknownClientHost"); //$NON-NLS-1$
        }
       
		LogonResult lr = this.createLogonResult(null, null, null, 0);
		this.doTestSocketServerConnectionProperties(p, e, lr, true);
	}

	/**
	 * Validate that the VDB name and version properties are being  
	 * set in the connection properties object after a <code>SocketServerConnection</code> 
	 * is established. 
	 * 
	 * <p>The expected result contains the VDB name and version for  
	 * both DQP and non-DQP properties. The initial connection 
	 * properties object only contains the VDB name for the non-DQP 
	 * property and no version number is defined. It is expected that 
	 * the connection properties will be updated to reflect the VDB 
	 * version returned by <code>LogonResult</code> and the VDB name 
	 * and version will be set for both DQP and non-DQP properties 
	 * within the connection properties object.</p>
	 * @throws Throwable 
	 *  
	 * @since Westport    
	 */
	public void testSocketServerConnection_PropertiesVdbVersion() throws Throwable {
		// for our pre connection properties
		Properties p = new Properties();
		p.setProperty(ConnectionProperties.VDB_NAME, "MyVDB"); //$NON-NLS-1$
		// for our post connection properties
		Properties e = new Properties();
		// the vdbName and vdbVersion are expected
		e.setProperty(ConnectionProperties.VDB_NAME, "MyVDB"); //$NON-NLS-1$
		e.setProperty(ConnectionProperties.VDB_VERSION, "4"); //$NON-NLS-1$
		e.setProperty(ConnectionProperties.VDB_NAME_DQP, "MyVDB"); //$NON-NLS-1$
		e.setProperty(ConnectionProperties.VDB_VERSION_DQP, "4"); //$NON-NLS-1$
		// for the productInfo properties to come back from LogonResult
		Properties pi = new Properties();
		// the vdbName and vdbVersion need to be set
		pi.setProperty(ProductInfoConstants.VIRTUAL_DB, "MyVDB"); //$NON-NLS-1$
		pi.setProperty(ProductInfoConstants.VDB_VERSION, "4"); //$NON-NLS-1$
		
		LogonResult lr = this.createLogonResult(null, null, pi, 0);
		this.doTestSocketServerConnectionProperties(p, e, lr, true);
	}

	/**
	 * Creates and returns a <code>LogonResult</code> object using 
	 * the specified parameters
	 * 
	 * <p>All parameters can be set to <code>null</code> if the 
	 * contents of the <code>LogonResult</code> is not relevant.</p>
	 *
	 * <p>The purpose of the method is as a helper to return a fully 
	 * constructed <code>LogonResult</code> object similar to what 
	 * would be returned by an implementation of the <code>ILogon.logon()</code> 
	 * method. 
	 *
	 * @param sessionID a session ID or <code>null</code>
	 * @param userName a user name (either unqualified or fully-qualified) or <code>null</code>
	 * @param productInfo a list of properties related to the target product or <code>null</code> - for example for Platform this might contain the properties for vdbName, vdbVersion, etc
	 * @param pingInterval the client ping interval - this is usually 0 
	 * @return A reference to a <code>LogonResult</code> object.
	 * @see LogonResult
	 * @since Westport
	 */
	private LogonResult createLogonResult(MetaMatrixSessionID sessionID, String userName, Properties productInfo, long pingInterval) {
		Properties pi = productInfo;
		if ( pi == null ) pi = new Properties();
		return new LogonResult(sessionID, userName, pi, pingInterval);
	}
	
	/**
	 * Helper test method which performs the creation of actual 
	 * values and can assert the expected values equal the actual 
	 * values
	 * 
	 * <p>The method creates an instance of <code>SocketServerConnection</code> 
	 * and provides the new instance with mock and overridden methods 
	 * for performing its initialization.</p>
	 * 
	 * During normal construction of <code>SocketServerConnection</code> 
	 * an implementation of <code>ILogon.logon()</code> is invoked.
	 * It is not expected that this implementation is needed for 
	 * testing <code>SocketServerConnection</code> but the 
	 * constructor does need the result of the invocation. This result 
	 * can be provided by <code>logonResult</code>.    
	 * 
	 * <p>A list of properties that represent the initial connection 
	 * properties are used in the initialization of <code>SocketServerConnection</code>
	 * and the expectation is that once <code>SocketServerConnection</code> 
	 * has been initialized the initial connection properties will 
	 * be updated to reflect status and state information found in 
	 * <code>logonResult</code>.</p>
	 * 
	 * <p>If <code>shouldAssert</code> the method will compare the 
	 * contents of <code>expectedProperties</code> to the contents 
	 * of <code>connectionProperties</code> and <code>fail()</code>
	 * the <code>TestCase</code> if one or more of the values in 
	 * <code>expectedProperties</code> does not match the 
	 * corresponding value in <code>connectionProperties</code> or 
	 * if the value is missing from <code>connectionProperties</code>.
	 * 
	 * <p>If not <code>shouldAssert</code> the comparison on 
	 * <code>expectedProperties</code> and <code>connectionProperties</code>
	 * is still performed but instead of invoking the <code>fail()</code> 
	 * method of the <code>TestCase</code> a report is returned 
	 * which contains the results of the comparison.  The report is 
	 * contained in a <code>ArrayList</code> with each failed 
	 * property comparison contained as an entry in the <code>ArrayList</code>.
	 * Each entry in the <code>ArrayList</code> is made up of a <code>String[]</code>
	 * array that contains three elements.  Element <code>[0]</code> 
	 * represents the property name while element <code>[1]</code> and 
	 * <code>[2]</code> represent the expected and actual values 
	 * respectively. If all expected values are accounted for and 
	 * were returned as actual values no report will exist and the 
	 * method will return <code>null</code>.   
	 * 
	 * @param connectionProperties a list of properties that represent an initial connection request
	 * @param expectedProperties a list of properties that represent the connection properties after a connection has been established
	 * @param logonResult a <code>LogonResult</code> object that will be returned to <code>SocketServerConnection</code> during its initialization
	 * @param shouldAssert <code>true if the <code>TestCase</code> should fail if one or more of the expected results doesn't match the actual results
	 * @return If <code>shouldAssert</code> is <code>true</code>, 
	 *         <code>null</code> will be returned. Otherwise, if all 
	 *         the expected results matched the actual results, 
	 *         <code>null</code> will be returned.  If one or more of 
	 *         the expected results did not match the actual results, 
	 *         a report will be returned as an <code>ArrayList</code> 
	 *         with each element in the list made up of 
	 *         <code>String[]{ "propertyName", "expectedValue", "actualValue" }</code>
	 * @throws Throwable
	 * @see SocketServerConnection#SocketServerConnection(SocketServerInstance, Properties, Timer)
	 * @see ILogon#logon()
	 * @see LogonResult
	 * @since Westport
	 */
	protected ArrayList<String[]> doTestSocketServerConnectionProperties(Properties connectionProperties, Properties expectedProperties, LogonResult logonResult, boolean shouldAssert) throws Throwable {
		// a mock of LogonImpl
		final ILogon logonImpl = Mockito.mock(ILogon.class);
		// a mock of SocketServerInstance as it is needed by SocketServerConnection
		SocketServerInstance ssi = Mockito.mock(SocketServerInstance.class);
		
		// the mock LogonImpl's logon method will always return logonResult
		Mockito.stub(logonImpl.logon(connectionProperties)).toReturn(logonResult);
		SocketServerConnection connection = null;
		// get a pseudo connection making sure to pass connection props that have an unqualified user name
		connection = new SocketServerConnection(ssi, connectionProperties, null) {
			// we need to override the getService method so that we can return our mock LogonImpl
			public <T> T getService(Class<T> iface) {
				if ( iface.getName().equals(ILogon.class.getName()) ) {
					return (T)logonImpl;
				}
				return null;
			}
		};

		if ( expectedProperties != null && connectionProperties != null ) {
			Enumeration<Object> ePropKeys = expectedProperties.keys();
			ArrayList<String[]> errorList = new ArrayList<String[]>();
			while ( ePropKeys.hasMoreElements() ) {
				String eKey = (String)ePropKeys.nextElement();
				String eValue = expectedProperties.getProperty(eKey);
				String value = connectionProperties.getProperty(eKey);
				if ( eValue != null && value != null ) {
					if ( !eValue.equals(value) ) errorList.add(new String[]{eKey, eValue, value});
				} else if ( eValue != value ) errorList.add(new String[]{eKey, eValue, value});
			}
			if ( errorList.size() > 0 ) {
				String failMessage = ""; //$NON-NLS-1$
				
				if ( shouldAssert ) {
					failMessage += "connectionProperties did not contain one or more of the expectedProperties:\n"; //$NON-NLS-1$
					for ( int i = 0; i < errorList.size(); i++ ) {
						failMessage += "   {Key: " + errorList.get(i)[0] + ", Expected: \"" + errorList.get(i)[1] + "\", Actual: \"" + errorList.get(i)[2] + "\"}\n"; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
					}
					failMessage = failMessage.trim();
					fail( failMessage );
				}
			}
			return errorList;
		} else if ( expectedProperties != connectionProperties ) {
			if ( shouldAssert ) {
				fail("Expected and actual Properties objects failed compariosn - Possibly one of them was <null>."); //$NON-NLS-1$
			}
			ArrayList<String[]> errorList = new ArrayList<String[]>();
			errorList.add( new String[]{"Properties", "expectedProperties", "connectionProperties"} ); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}
		return null;
	}
}
