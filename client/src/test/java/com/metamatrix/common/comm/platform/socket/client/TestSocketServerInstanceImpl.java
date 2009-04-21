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
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.socket.Handshake;
import com.metamatrix.common.comm.platform.socket.ObjectChannel;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;

public class TestSocketServerInstanceImpl extends TestCase {
	
	private static class FakeObjectChannel implements ObjectChannel, ObjectChannelFactory {
		List<Object> msgs = new ArrayList<Object>();
		List<? extends Object> readMsgs;
		int readCount;
		
		public FakeObjectChannel(List<? extends Object> readMsgs) {
			this.readMsgs = readMsgs;
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public void close() {
			
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public boolean isOpen() {
			return true;
		}

		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public Future<?> write(Object msg) {
			msgs.add(msg);
			ResultsFuture<?> result = new ResultsFuture<Void>();
			result.getResultsReceiver().receiveResults(null);
			return result;
		}
		
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public Object read() throws IOException,
				ClassNotFoundException {
			Object msg = readMsgs.get(readCount++);
			if (msg instanceof IOException) {
				if (msg instanceof SocketTimeoutException) {
					try {
						Thread.sleep(5);
					} catch (InterruptedException e) {
					}
				}
				throw (IOException)msg;
			}
			return msg;
		}
		
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public SocketAddress getRemoteAddress() {
			return null;
		}
		
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public ObjectChannel createObjectChannel(SocketAddress address,
				boolean ssl) throws IOException, CommunicationException {
			return this;
		}
		
		//## JDBC4.0-begin ##
		@Override
		//## JDBC4.0-end ##
		public int getSoTimeout() {
			return 1;
		}
		
	}

	public void testHandshakeTimeout() throws Exception {
		final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(new SocketTimeoutException()));
		
		try {
			createInstance(channel);
			fail("Exception expected"); //$NON-NLS-1$
		} catch (IOException e) {
			
		}
	}

	private SocketServerInstanceImpl createInstance(ObjectChannelFactory channelFactory)
			throws CommunicationException, IOException {
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(new HostInfo("0.0.0.0", 1), false, 1); //$NON-NLS-1$
		ssii.connect(channelFactory);
		return ssii;
	}
	
	public void testSuccessfulHandshake() throws Exception {
		final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(new Handshake(), new SocketTimeoutException()));
		
		SocketServerInstanceImpl instance = createInstance(channel);
		
		//no remote server is hooked up, so this will timeout
		ILogon logon = instance.getService(ILogon.class);
		try {
			logon.logon(new Properties());
			fail("Exception expected"); //$NON-NLS-1$
		} catch (MetaMatrixComponentException e) {
			assertTrue(e.getCause().getCause() instanceof TimeoutException);
		}
	}
	
	public void testVersionMismatch() throws Exception {
		Handshake h = new Handshake();
		h.setVersion("foo"); //$NON-NLS-1$
		final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(h));
		try {
			createInstance(channel);
			fail("exception expected"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			assertTrue(e.getMessage().startsWith("Handshake failed due to version mismatch")); //$NON-NLS-1$
		}
	}

}
