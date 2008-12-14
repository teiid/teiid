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

package com.metamatrix.common.comm.platform.socket.client;

import java.io.IOException;
import java.net.SocketAddress;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLEngine;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.common.comm.platform.socket.SocketLog;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListener;
import com.metamatrix.common.comm.platform.socket.ObjectChannel.ChannelListenerFactory;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;

public class TestSocketServerInstanceImpl extends TestCase {
	
	private static final class FakeObjectChannel implements ObjectChannel {
		Object msg;

		@Override
		public void close() {
			
		}

		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public Future<?> write(Object msg) {
			ResultsFuture<?> result = new ResultsFuture();
			this.msg = msg;
			result.getResultsReceiver().receiveResults(null);
			return result;
		}
	}

	public void testHandshakeTimeout() throws Exception {
		ObjectChannelFactory channelFactory = Mockito.mock(ObjectChannelFactory.class);
		try {
			createInstance(channelFactory);
			fail("Exception expected");
		} catch (CommunicationException e) {
			assertEquals("Handshake timeout", e.getMessage());
		}
	}

	private SocketServerInstanceImpl createInstance(ObjectChannelFactory channelFactory)
			throws CommunicationException, IOException {
		return new SocketServerInstanceImpl(new HostInfo("foo", 1), false, Mockito.mock(SocketLog.class), channelFactory, 1, 1);
	}
	
	public void testSuccessfulHandshake() throws Exception {
		final FakeObjectChannel channel = new FakeObjectChannel();
		ObjectChannelFactory channelFactory = new ObjectChannelFactory() {
			@Override
			public void createObjectChannel(SocketAddress address,
					SSLEngine engine, ChannelListenerFactory listenerFactory)
					throws IOException, CommunicationException {
				assertNull(engine);
				ChannelListener listener = listenerFactory.createChannelListener(channel);
				listener.receivedMessage(new Handshake());
				assertTrue(channel.msg instanceof Handshake);
			}
		};
		SocketServerInstanceImpl instance = createInstance(channelFactory);
		ILogon logon = instance.getService(ILogon.class);
		try {
			logon.logon(new Properties());
			fail("Exception expected");
		} catch (MetaMatrixComponentException e) {
			assertTrue(e.getCause() instanceof TimeoutException);
		}
	}

}
