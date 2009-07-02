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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.net.InetSocketAddress;
import java.util.Properties;

import javax.net.ssl.SSLEngine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnection;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.comm.platform.socket.client.UrlServerDiscovery;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;
import com.metamatrix.platform.vm.controller.SocketListenerStats;

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
		ClientServiceRegistry csr = new ClientServiceRegistry();
		SessionServiceInterface sessionService = mock(SessionServiceInterface.class);
		csr.registerClientService(ILogon.class, new LogonImpl(sessionService, "fakeCluster"), "foo"); //$NON-NLS-1$ //$NON-NLS-2$
		listener = new SocketListener(addr.getPort(), addr.getAddress().getHostAddress(),
				csr, 1024, 1024, 1, null, true, sessionService);

		try {
			Properties p = new Properties();
			String url = new MMURL(addr.getHostName(), listener.getPort() - 1, false).getAppServerURL();
			p.setProperty(MMURL.CONNECTION.SERVER_URL, url); //wrong port
			SocketServerConnectionFactory.getInstance().createConnection(p);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {

		}
	}

	@Test public void testConnectWithoutPooling() throws Exception {
		Properties p = new Properties();
		p.setProperty("org.teiid.sockets.maxCachedInstances", String.valueOf(0)); //$NON-NLS-1$
		SocketServerConnection conn = helpEstablishConnection(false, null, true, p);
		SocketListenerStats stats = listener.getStats();
		assertEquals(2, stats.objectsRead); // handshake response, logon,
		assertEquals(1, stats.sockets);
		conn.shutdown();
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
		SocketServerConnection conn = helpEstablishConnection(false, null);
		SocketListenerStats stats = listener.getStats();
		assertEquals(2, stats.objectsRead); // handshake response, logon,
		assertEquals(1, stats.sockets);
		conn.shutdown();
		stats = listener.getStats();
		assertEquals(1, stats.maxSockets);
		assertEquals(3, stats.objectsRead); // handshake response, logon, logoff
		stats = listener.getStats();
		assertEquals(1, stats.sockets);
		conn = helpEstablishConnection(false, null);
		conn.shutdown();
		stats = listener.getStats();
		assertEquals(1, stats.sockets);
		assertEquals(1, stats.maxSockets);
	}


	@Test public void testConnectWithoutClientEncryption() throws Exception {
		SocketServerConnection conn = helpEstablishConnection(false, null, false, new Properties());
		assertTrue(conn.selectServerInstance().getCryptor() instanceof NullCryptor);
		conn.shutdown();
	}

	private SocketServerConnection helpEstablishConnection(boolean secure,
			SSLEngine serverSSL) throws CommunicationException, ConnectionException {
		return helpEstablishConnection(secure, serverSSL, true, new Properties());
	}

	private SocketServerConnection helpEstablishConnection(boolean secure,
			SSLEngine serverSSL, boolean isClientEncryptionEnabled, Properties socketConfig) throws CommunicationException,
			ConnectionException {
		if (listener == null) {
			SessionServiceInterface sessionService = mock(SessionServiceInterface.class);
			ClientServiceRegistry csr = new ClientServiceRegistry();
			csr.registerClientService(ILogon.class, new LogonImpl(sessionService, "fakeCluster") { //$NON-NLS-1$
				@Override
				public LogonResult logon(Properties connProps)
						throws LogonException, ComponentNotFoundException {
					return new LogonResult();
				}
			}, "foo"); //$NON-NLS-1$
			listener = new SocketListener(addr.getPort(), addr.getAddress().getHostAddress(),
					csr, 1024, 1024, 1, serverSSL, isClientEncryptionEnabled, sessionService);
			SocketListenerStats stats = listener.getStats();
			assertEquals(0, stats.maxSockets);
			assertEquals(0, stats.objectsRead);
			assertEquals(0, stats.objectsWritten);
			assertEquals(0, stats.sockets);
		}

		Properties p = new Properties();
		String url = new MMURL(addr.getHostName(), listener.getPort(),secure).getAppServerURL();
		p.setProperty(MMURL.CONNECTION.SERVER_URL, url); 
		p.setProperty(MMURL.CONNECTION.DISCOVERY_STRATEGY, UrlServerDiscovery.class.getName());
		if (sscf == null) {
			sscf = new SocketServerConnectionFactory();
			sscf.init(socketConfig);
		}
		return sscf.createConnection(p);
	}

	@Test public void testSSLConnectWithNonSSLServer() throws Exception {
		try {
			helpEstablishConnection(true, null);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			
		}
	}

	@Test public void testAnonSSLConnect() throws Exception {
		SSLEngine engine = SocketUtil.getAnonSSLContext().createSSLEngine();
		engine.setUseClientMode(false);
		engine.setEnabledCipherSuites(new String[] { SocketUtil.ANON_CIPHER_SUITE });
		Properties p = new Properties();
		p.setProperty(SocketUtil.TRUSTSTORE_FILENAME, SocketUtil.NONE);
		SocketServerConnection conn = helpEstablishConnection(true, engine, true, p);
		conn.shutdown();
	}
	
}
