/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
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
package org.teiid.transport;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnection;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.comm.platform.socket.client.UrlServerDiscovery;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.service.SessionService;

public class TestCommSockets {

	SocketListener listener;
	private SocketServerConnectionFactory sscf;
	private InetSocketAddress addr;

	@Before public void setUp() {
		addr = new InetSocketAddress(0);
	}
	
	@After public void tearDown() throws Exception {
		if (listener != null) {
			listener.stop();
		}
	}

	@Test public void testFailedConnect() throws Exception {
		SSLConfiguration config = new SSLConfiguration();
		listener = new SocketListener(addr.getPort(), addr.getAddress().getHostAddress(),1024, 1024, 1, config, null);

		try {
			Properties p = new Properties();
			String url = new MMURL(addr.getHostName(), listener.getPort() - 1, false).getAppServerURL();
			p.setProperty(MMURL.CONNECTION.SERVER_URL, url); //wrong port
			SocketServerConnectionFactory.getInstance().getConnection(p);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {

		}
	}

	@Test public void testConnectWithoutPooling() throws Exception {
		Properties p = new Properties();
		p.setProperty("org.teiid.sockets.maxCachedInstances", String.valueOf(0)); //$NON-NLS-1$
		SocketServerConnection conn = helpEstablishConnection(false, new SSLConfiguration(), p);
		SocketListenerStats stats = listener.getStats();
		assertEquals(2, stats.objectsRead); // handshake response, logon,
		assertEquals(1, stats.sockets);
		conn.close();
		stats = listener.getStats();
		assertEquals(1, stats.maxSockets);
		assertEquals(3, stats.objectsRead); // handshake response, logon, logoff
		if (stats.sockets > 0) {
			// there is a timing issue here, since the effect of shutdown on the
			// server side can be delayed
			Thread.sleep(500);
		}
		stats = listener.getStats();
		assertEquals(0, stats.sockets);
	}
	
	@Test public void testConnectWithPooling() throws Exception {
		SocketServerConnection conn = helpEstablishConnection(false);
		SocketListenerStats stats = listener.getStats();
		assertEquals(2, stats.objectsRead); // handshake response, logon,
		assertEquals(1, stats.sockets);
		conn.close();
		stats = listener.getStats();
		assertEquals(1, stats.maxSockets);
		assertEquals(3, stats.objectsRead); // handshake response, logon, logoff
		stats = listener.getStats();
		assertEquals(1, stats.sockets);
		conn = helpEstablishConnection(false);
		conn.close();
		stats = listener.getStats();
		assertEquals(1, stats.sockets);
		assertEquals(1, stats.maxSockets);
	}


	@Test public void testConnectWithoutClientEncryption() throws Exception {
		SSLConfiguration config = new SSLConfiguration();
		config.setClientEncryptionEnabled(false);
		SocketServerConnection conn = helpEstablishConnection(false, config, new Properties());
		assertTrue(conn.selectServerInstance().getCryptor() instanceof NullCryptor);
		conn.close();
	}

	private SocketServerConnection helpEstablishConnection(boolean secure) throws CommunicationException, ConnectionException {
		return helpEstablishConnection(secure, new SSLConfiguration(), new Properties());
	}

	private SocketServerConnection helpEstablishConnection(boolean clientSecure, SSLConfiguration config, Properties socketConfig) throws CommunicationException,
			ConnectionException {
		if (listener == null) {
			ClientServiceRegistryImpl server = new ClientServiceRegistryImpl();
			server.registerClientService(ILogon.class, new LogonImpl(mock(SessionService.class), "fakeCluster") { //$NON-NLS-1$
				@Override
				public LogonResult logon(Properties connProps)
						throws LogonException, ComponentNotFoundException {
					return new LogonResult();
				}

			}, null); 
			listener = new SocketListener(addr.getPort(), addr.getAddress().getHostAddress(), 1024, 1024, 1, config, server);
			
			SocketListenerStats stats = listener.getStats();
			assertEquals(0, stats.maxSockets);
			assertEquals(0, stats.objectsRead);
			assertEquals(0, stats.objectsWritten);
			assertEquals(0, stats.sockets);
		}

		Properties p = new Properties();
		String url = new MMURL(addr.getHostName(), listener.getPort(), clientSecure).getAppServerURL();
		p.setProperty(MMURL.CONNECTION.SERVER_URL, url); 
		p.setProperty(MMURL.CONNECTION.DISCOVERY_STRATEGY, UrlServerDiscovery.class.getName());
		if (sscf == null) {
			sscf = new SocketServerConnectionFactory();
			sscf.initialize(socketConfig);
		}
		return sscf.getConnection(p);
	}

	@Test public void testSSLConnectWithNonSSLServer() throws Exception {
		try {
			helpEstablishConnection(true);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			
		}
	}

	@Test public void testAnonSSLConnect() throws Exception {
		SSLConfiguration config = new SSLConfiguration();
		config.setSslEnabled(true);
		config.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
		Properties p = new Properties();
		p.setProperty(SocketUtil.TRUSTSTORE_FILENAME, SocketUtil.NONE);
		try {
			helpEstablishConnection(false, config, p);
		} catch (CommunicationException e) {
			
		}
		SocketServerConnection conn = helpEstablishConnection(true, config, p);
		conn.close();
	}
	
}
