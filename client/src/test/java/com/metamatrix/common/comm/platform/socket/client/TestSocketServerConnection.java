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

/**
 * 
 */
package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.exception.SingleInstanceCommunicationException;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.SessionToken;

/**
 * <code>TestCase</case> for <code>SocketServerConnection</code>
 * @see SocketServerConnection
 * @since Westport
 */
public class TestSocketServerConnection extends TestCase {
	
	private static final class FakeILogon implements ILogon {
		
		Throwable t;
		
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public void assertIdentity(SessionToken sessionId)
				throws InvalidSessionException, MetaMatrixComponentException {
			
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public ResultsFuture<?> logoff()
				throws InvalidSessionException,
				MetaMatrixComponentException {
			return null;
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public LogonResult logon(
				Properties connectionProperties)
				throws LogonException,
				MetaMatrixComponentException {
			return new LogonResult(new SessionToken(new MetaMatrixSessionID(1), "fooUser"), new Properties(), "fake"); //$NON-NLS-1$ //$NON-NLS-2$
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
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
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public SocketServerInstance getServerInstance(HostInfo info,
					boolean ssl) throws CommunicationException, IOException {
				throw new SingleInstanceCommunicationException();
			}
		};
		ServerDiscovery discovery = new UrlServerDiscovery(new MMURL("mm://host1:1,host2:2")); //$NON-NLS-1$
		try {
			new SocketServerConnection(instanceFactory, false, discovery, p, null);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			assertEquals("No valid host available. Attempted connections to: [host1:1, host2:2]", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	public void testLogon() throws Exception {
		SocketServerConnection connection = createConnection(null);
		assertEquals("1", connection.getLogonResult().getSessionID().toString()); //$NON-NLS-1$
	}
	
	/**
	 * Since the original instance is still open, this will be a transparent retry
	 */
	public void testRetry() throws Exception {
		SocketServerConnection connection = createConnection(new SingleInstanceCommunicationException());
		connection.setFailOver(true);
		ILogon logon = connection.getService(ILogon.class);
		logon.ping();
	}
	
	public void testImmediateFail() throws Exception {
		SocketServerConnection connection = createConnection(new CommunicationException());
		ILogon logon = connection.getService(ILogon.class);
		try {
			logon.ping();
			fail("expected exception"); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			
		}
	}
	
	public void testImmediateFail1() throws Exception {
		SocketServerConnection connection = createConnection(new CommunicationException());
		connection.setFailOver(true);
		ILogon logon = connection.getService(ILogon.class);
		try {
			logon.ping();
			fail("expected exception"); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			
		}
	}

	private SocketServerConnection createConnection(final Throwable throwException) throws CommunicationException, ConnectionException {
		return createConnection(throwException, new HostInfo("0.0.0.2", 1)); //$NON-NLS-1$
	}
	
	private SocketServerConnection createConnection(final Throwable t, HostInfo hostInfo)
			throws CommunicationException, ConnectionException {
		Properties p = new Properties();
		ServerDiscovery discovery = new UrlServerDiscovery(new MMURL(hostInfo.getHostName(), hostInfo.getPortNumber(), false));
		SocketServerInstanceFactory instanceFactory = new SocketServerInstanceFactory() {
			//## JDBC4.0-begin ##
			@Override
			//## JDBC4.0-end ##
			public SocketServerInstance getServerInstance(final HostInfo info,
					boolean ssl) throws CommunicationException, IOException {
				SocketServerInstance instance = Mockito.mock(SocketServerInstance.class);
				Mockito.stub(instance.getCryptor()).toReturn(new NullCryptor());
				Mockito.stub(instance.getRemoteAddress()).toReturn(new InetSocketAddress(info.getInetAddress(), info.getPortNumber()));
				FakeILogon logon = new FakeILogon();
				logon.t = t;
				Mockito.stub(instance.getService(ILogon.class)).toReturn(logon);
				Mockito.stub(instance.isOpen()).toReturn(true);
				return instance;
			}
		};
		SocketServerConnection connection = new SocketServerConnection(instanceFactory, false, discovery, p, null);
		return connection;
	}
	
	public void testIsSameInstance() throws Exception {
		SocketServerConnection conn = createConnection(null, new HostInfo("0.0.0.0", 1)); //$NON-NLS-1$
		SocketServerConnection conn1 = createConnection(null, new HostInfo("0.0.0.1", 1)); //$NON-NLS-1$
		
		assertFalse(conn.isSameInstance(conn1));
		assertTrue(conn.isSameInstance(conn));
	}

}
