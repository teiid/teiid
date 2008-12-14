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

import java.io.IOException;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

/**
 * <code>TestCase</case> for <code>SocketServerConnection</code>
 * @see SocketServerConnection
 * @since Westport
 */
public class TestSocketServerConnection extends TestCase {
	
	private static final class FakeILogon implements ILogon {
		
		Throwable t;
		
		@Override
		public void assertIdentity(
				MetaMatrixSessionID sessionId)
				throws InvalidSessionException,
				MetaMatrixComponentException {
			
		}

		@Override
		public ResultsFuture<?> logoff()
				throws InvalidSessionException,
				MetaMatrixComponentException {
			return null;
		}

		@Override
		public LogonResult logon(
				Properties connectionProperties)
				throws LogonException,
				MetaMatrixComponentException {
			return new LogonResult(new MetaMatrixSessionID(1), "fooUser", new Properties(), 1, "fake"); //$NON-NLS-1$
		}

		@Override
		public ResultsFuture<?> ping()
				throws InvalidSessionException,
				MetaMatrixComponentException {
			if (t != null) {
				MetaMatrixComponentException e = new MetaMatrixComponentException(t);
				t = null;
				throw e;
			}
			return null;
		}
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
		Properties p = new Properties();
		
		SocketServerConnectionFactory.updateConnectionProperties(p);
       
		assertTrue(p.containsKey(MMURL.CONNECTION.CLIENT_HOSTNAME));
		assertTrue(p.containsKey(MMURL.CONNECTION.CLIENT_IP_ADDRESS));
	}
	
	public void testLogonFailsWithMultipleHosts() throws Exception {
		Properties p = new Properties();
		SocketServerInstanceFactory instanceFactory = new SocketServerInstanceFactory() {
			@Override
			public SocketServerInstance createServerInstance(HostInfo info,
					boolean ssl) throws CommunicationException, IOException {
				throw new SingleInstanceCommunicationException();
			}
		};
		ServerDiscovery discovery = new UrlServerDiscovery(new MMURL("mm://host1:1,host2:2"));
		try {
			new SocketServerConnection(instanceFactory, false, discovery, p, null, Mockito.mock(SocketLog.class));
			fail("exception expected");
		} catch (CommunicationException e) {
			assertEquals("No valid host available. Attempted connections to: [host1:1, host2:2]", e.getMessage());
		}
	}
	
	public void testLogon() throws Exception {
		SocketServerConnection connection = createConnection(null);
		assertEquals("00000000-0000-0001-0000-000000000001", connection.getLogonResult().getSessionID().toString());
	}
	
	/**
	 * Since the original instance is still open, this will be a transparent retry
	 */
	public void testRetry() throws Exception {
		SocketServerConnection connection = createConnection(new SingleInstanceCommunicationException());
		ILogon logon = connection.getService(ILogon.class);
		logon.ping();
	}
	
	public void testImmediateFail() throws Exception {
		SocketServerConnection connection = createConnection(new CommunicationException());
		ILogon logon = connection.getService(ILogon.class);
		try {
			logon.ping();
			fail("expected exception");
		} catch (MetaMatrixComponentException e) {
			
		}
	}

	private SocketServerConnection createConnection(final Throwable throwException) throws CommunicationException, ConnectionException {
		return createConnection(throwException, new HostInfo("foo", 1));
	}
	
	private SocketServerConnection createConnection(final Throwable t, HostInfo hostInfo)
			throws CommunicationException, ConnectionException {
		Properties p = new Properties();
		ServerDiscovery discovery = new UrlServerDiscovery(new MMURL(hostInfo.getHostName(), hostInfo.getPortNumber(), false));
		SocketServerInstanceFactory instanceFactory = new SocketServerInstanceFactory() {
			@Override
			public SocketServerInstance createServerInstance(final HostInfo info,
					boolean ssl) throws CommunicationException, IOException {
				SocketServerInstance instance = Mockito.mock(SocketServerInstance.class);
				Mockito.stub(instance.getCryptor()).toReturn(new NullCryptor());
				Mockito.stub(instance.getHostInfo()).toReturn(info);
				FakeILogon logon = new FakeILogon();
				logon.t = t;
				Mockito.stub(instance.getService(ILogon.class)).toReturn(logon);
				Mockito.stub(instance.isOpen()).toReturn(true);
				return instance;
			}
		};
		SocketServerConnection connection = new SocketServerConnection(instanceFactory, false, discovery, p, null, Mockito.mock(SocketLog.class));
		return connection;
	}
	
	public void testIsSameInstance() throws Exception {
		SocketServerConnection conn = createConnection(null, new HostInfo("foo", 1));
		SocketServerConnection conn1 = createConnection(null, new HostInfo("bar", 1));
		
		ClientSideDQP dqp = conn.getService(ClientSideDQP.class);
		ClientSideDQP dqp1 = conn1.getService(ClientSideDQP.class);
		
		assertFalse(SocketServerConnection.isSameInstance(dqp, dqp1));
		assertTrue(SocketServerConnection.isSameInstance(dqp, dqp));
	}

}
