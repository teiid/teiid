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
			fail("Exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			assertEquals("Handshake timeout", e.getMessage()); //$NON-NLS-1$
		}
	}

	private SocketServerInstanceImpl createInstance(ObjectChannelFactory channelFactory)
			throws CommunicationException, IOException {
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(new HostInfo("foo", 1), null, Mockito.mock(SocketLog.class), 1); //$NON-NLS-1$
		ssii.connect(channelFactory, 1);
		return ssii;
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
		
		//no remote server is hooked up, so this will timeout
		ILogon logon = instance.getService(ILogon.class);
		try {
			logon.logon(new Properties());
			fail("Exception expected"); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			assertTrue(e.getCause() instanceof TimeoutException);
		}
	}
	
	public void testVersionMismatch() throws Exception {
		final FakeObjectChannel channel = new FakeObjectChannel();
		ObjectChannelFactory channelFactory = new ObjectChannelFactory() {
			@Override
			public void createObjectChannel(SocketAddress address,
					SSLEngine engine, ChannelListenerFactory listenerFactory)
					throws IOException, CommunicationException {
				assertNull(engine);
				ChannelListener listener = listenerFactory.createChannelListener(channel);
				Handshake h = new Handshake();
				h.setVersion("foo"); //$NON-NLS-1$
				listener.receivedMessage(h);
			}
		};
		try {
			createInstance(channelFactory);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			e.printStackTrace();
			assertEquals("Handshake failed due to version mismatch -- Client Version: 6.0, Server Version: foo", e.getMessage()); //$NON-NLS-1$
		}
	}


}
