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
package org.teiid.net.socket;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.TeiidURL;


/**
 * <code>TestCase</case> for <code>SocketServerConnection</code>
 * @see SocketServerConnection
 */
@SuppressWarnings("nls")
public class TestSocketServerConnection {
	
	private static final class FakeILogon implements ILogon {
		
		Throwable t;
		
		public FakeILogon(Throwable t) {
			this.t = t;
		}
		
		@Override
		public void assertIdentity(SessionToken sessionId)
				throws InvalidSessionException, TeiidComponentException {
			
		}

		@Override
		public ResultsFuture<?> logoff()
				throws InvalidSessionException {
			return null;
		}

		@Override
		public LogonResult logon(
				Properties connectionProperties)
				throws LogonException,
				TeiidComponentException {
			return new LogonResult(new SessionToken(1, connectionProperties.getProperty(TeiidURL.CONNECTION.USER_NAME, "fooUser")), "foo", 1, "fake"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		}

		@Override
		public ResultsFuture<?> ping()
				throws TeiidComponentException, CommunicationException {
			if (t != null) {
				if (t instanceof CommunicationException) {
					CommunicationException ce = (CommunicationException)t;
					t = null;
					throw ce;
				}
				TeiidComponentException e = new TeiidComponentException(t);
				t = null;
				throw e;
			}
			return ResultsFuture.NULL_FUTURE;
		}
		
		@Override
		public ResultsFuture<?> ping(Collection<String> sessions)
				throws TeiidComponentException, CommunicationException {
			return ping();
		}

		@Override
		public LogonResult neogitiateGssLogin(Properties connectionProperties,
				byte[] serviceToken, boolean createSession) throws LogonException {
			return null;
		}

		@Override
		public AuthenticationType getAuthenticationType(String vdbName,
				String version, AuthenticationType preferType) {
			return AuthenticationType.USERPASSWORD;
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
	@Test public void testSocketServerConnection_PropertiesClientHost() throws Throwable {
		Properties p = new Properties();
		
		SocketServerConnection.updateConnectionProperties(p, InetAddress.getLocalHost(), true);
       
		assertTrue(p.containsKey(TeiidURL.CONNECTION.CLIENT_HOSTNAME));
		assertTrue(p.containsKey(TeiidURL.CONNECTION.CLIENT_IP_ADDRESS));
	}
	
	@Test public void testLogonFailsWithMultipleHosts() throws Exception {
		Properties p = new Properties();
		SocketServerInstanceFactory instanceFactory = Mockito.mock(SocketServerInstanceFactory.class);
		Mockito.stub(instanceFactory.getServerInstance((HostInfo)Mockito.anyObject())).toThrow(new SingleInstanceCommunicationException());
		ServerDiscovery discovery = new UrlServerDiscovery(new TeiidURL("mm://host1:1,host2:2")); //$NON-NLS-1$
		try {
			new SocketServerConnection(instanceFactory, false, discovery, p);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			assertEquals("TEIID20021 No valid host available. Attempted connections to: [host1:1, host2:2]", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	@Test public void testLogon() throws Exception {
		SocketServerConnection connection = createConnection(null);
		assertEquals(String.valueOf(1), connection.getLogonResult().getSessionID()); 
	}
	
	@Test public void testChangeUser() throws Exception {
		Properties p = new Properties();
		SocketServerConnection connection = createConnection(null, p);
		assertEquals("fooUser", connection.getLogonResult().getUserName()); 
		p.setProperty(TeiidURL.CONNECTION.USER_NAME, "newUser");
		connection.authenticate();
		assertEquals("newUser", connection.getLogonResult().getUserName());
	}
	
	/**
	 * Since the original instance is still open, this will be a transparent retry
	 */
	@Test public void testRetry() throws Exception {
		SocketServerConnection connection = createConnection(new SingleInstanceCommunicationException());
		connection.setFailOver(true);
		connection.setFailOverPingInterval(50);
		ILogon logon = connection.getService(ILogon.class);
		Thread.sleep(70);
		logon.ping();
	}
	
	@Test(expected=CommunicationException.class) public void testImmediateFail() throws Exception {
		SocketServerConnection connection = createConnection(new CommunicationException());
		ILogon logon = connection.getService(ILogon.class);
		logon.ping();
	}
	
	@Test(expected=CommunicationException.class) public void testImmediateFail1() throws Exception {
		SocketServerConnection connection = createConnection(new CommunicationException());
		connection.setFailOver(true);
		ILogon logon = connection.getService(ILogon.class);
		logon.ping();
	}

	private SocketServerConnection createConnection(final Throwable throwException) throws CommunicationException, ConnectionException {
		return createConnection(throwException, new Properties());
	}
	
	private SocketServerConnection createConnection(final Throwable throwException, Properties p) throws CommunicationException, ConnectionException {
		return createConnection(throwException, new HostInfo("0.0.0.2", 1), p); //$NON-NLS-1$
	}
	
	private SocketServerConnection createConnection(final Throwable t, final HostInfo hostInfo, Properties p)
			throws CommunicationException, ConnectionException {
		ServerDiscovery discovery = new UrlServerDiscovery(new TeiidURL(hostInfo.getHostName(), hostInfo.getPortNumber(), false));
		SocketServerInstanceFactory instanceFactory = new SocketServerInstanceFactory() {
			FakeILogon logon = new FakeILogon(t);
			
			@Override
			public SocketServerInstance getServerInstance(HostInfo info)
					throws CommunicationException, IOException {
				SocketServerInstance instance = Mockito.mock(SocketServerInstance.class);
				Mockito.stub(instance.getCryptor()).toReturn(new NullCryptor());
				Mockito.stub(instance.getHostInfo()).toReturn(hostInfo);
				Mockito.stub(instance.getService(ILogon.class)).toReturn(logon);
				Mockito.stub(instance.getServerVersion()).toReturn("07.03");
				if (t != null) {
					try {
						Mockito.doAnswer(new Answer<Void>() {
							@Override
							public Void answer(InvocationOnMock invocation)
									throws Throwable {
								if (logon.t == null) {
									return null;
								}
								throw logon.t;
							}
						}).when(instance).send((Message)Mockito.anyObject(), (ResultsReceiver<Object>)Mockito.anyObject(), (Serializable)Mockito.anyObject());
					} catch (Exception e) {
						
					}
				}
				Mockito.stub(instance.isOpen()).toReturn(true);
				return instance;
			}
			
			@Override
			public void connected(SocketServerInstance instance,
					SessionToken session) {
				
			}
			
			@Override
			public void disconnected(SocketServerInstance instance,
					SessionToken session) {
				
			}
		};
		SocketServerConnection connection = new SocketServerConnection(instanceFactory, false, discovery, p);
		return connection;
	}
	
	@Test public void testIsSameInstance() throws Exception {
		SocketServerConnection conn = createConnection(null, new HostInfo("0.0.0.0", 1), new Properties()); //$NON-NLS-1$
		SocketServerConnection conn1 = createConnection(null, new HostInfo("0.0.0.1", 1), new Properties()); //$NON-NLS-1$
		
		assertFalse(conn.isSameInstance(conn1));
		assertTrue(conn.isSameInstance(conn));
	}

}
