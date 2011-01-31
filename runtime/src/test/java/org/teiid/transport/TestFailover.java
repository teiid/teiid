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
import org.junit.Test;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.ComponentNotFoundException;
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.service.SessionService;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.net.socket.SocketServerConnectionFactory;
import org.teiid.net.socket.UrlServerDiscovery;
import org.teiid.transport.TestSocketRemoting.FakeService;


@SuppressWarnings("nls")
public class TestFailover {

	SocketListener listener;
	SocketListener listener1;

	private SocketServerConnectionFactory sscf;
	private InetSocketAddress addr = new InetSocketAddress(0);
	private int logonAttempts;
	
	@After public void tearDown() {
		if (this.listener != null) {
			this.listener.stop();
		}
		if (this.listener1 != null) {
			this.listener1.stop();
		}
	}

	private SocketServerConnection helpEstablishConnection(boolean clientSecure, SSLConfiguration config, Properties socketConfig) throws CommunicationException,
			ConnectionException {
		listener = createListener(addr, config);
		listener1 = createListener(addr, config);
		listener1.stop();
		Properties p = new Properties();
		TeiidURL teiidUrl = new TeiidURL(addr.getHostName(), listener.getPort(), clientSecure);
		teiidUrl.getHostInfo().add(new HostInfo(addr.getHostName(), listener1.getPort()));
		String url = teiidUrl.getAppServerURL();
		p.setProperty(TeiidURL.CONNECTION.SERVER_URL, url); 
		p.setProperty(TeiidURL.CONNECTION.DISCOVERY_STRATEGY, UrlServerDiscovery.class.getName());
		p.setProperty(TeiidURL.CONNECTION.AUTO_FAILOVER, Boolean.TRUE.toString());
		if (sscf == null) {
			sscf = new SocketServerConnectionFactory();
			sscf.initialize(socketConfig);
		}
		return sscf.getConnection(p);
	}

	private SocketListener createListener(InetSocketAddress address, SSLConfiguration config) {
		ClientServiceRegistryImpl server = new ClientServiceRegistryImpl();
		server.registerClientService(ILogon.class, new LogonImpl(mock(SessionService.class), "fakeCluster") { //$NON-NLS-1$
			@Override
			public LogonResult logon(Properties connProps)
					throws LogonException, ComponentNotFoundException {
				logonAttempts++;
				return new LogonResult(new SessionToken("dummy"), "x", 1, "z");
			}
			
			@Override
			public ResultsFuture<?> ping() throws InvalidSessionException,
					TeiidComponentException {
				return ResultsFuture.NULL_FUTURE;
			}
			
			@Override
			public void assertIdentity(SessionToken checkSession)
					throws InvalidSessionException, TeiidComponentException {
				throw new InvalidSessionException();
			}

		}, null); 
		server.registerClientService(FakeService.class, new TestSocketRemoting.FakeServiceImpl(), null);
		return new SocketListener(address.getPort(), address.getAddress().getHostAddress(), 1024, 1024, 1, config, server, BufferManagerFactory.getStandaloneBufferManager());
	}
	
	@Test public void testFailover() throws Exception {
		SSLConfiguration config = new SSLConfiguration();
		Properties p = new Properties();
		SocketServerConnection conn = helpEstablishConnection(false, config, p);
		assertTrue(conn.isOpen(1000));
		//restart the second instance now that we know the connection was made to the first
		listener1 = createListener(new InetSocketAddress(addr.getAddress(), listener1.getPort()), config);
		listener.stop();
		conn.isOpen(1000); //there is a chance this call can fail
		assertTrue(conn.isOpen(1000));
		listener1.stop();
		//both instances are down
		assertFalse(conn.isOpen(1000));
		//bring the first back up
		listener = createListener(new InetSocketAddress(addr.getAddress(), listener.getPort()), config);
		assertTrue(conn.isOpen(1000));
		assertEquals(3, logonAttempts);
		conn.close();
	}
	
}
