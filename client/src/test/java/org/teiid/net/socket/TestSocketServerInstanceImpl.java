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

package org.teiid.net.socket;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.junit.Test;
import org.teiid.client.security.ILogon;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.TeiidComponentException;
import org.teiid.net.CommunicationException;
import org.teiid.net.HostInfo;

@SuppressWarnings("nls")
public class TestSocketServerInstanceImpl {
	
	private static class FakeObjectChannel implements ObjectChannel, ObjectChannelFactory {
		List<Object> msgs = new ArrayList<Object>();
		List<? extends Object> readMsgs;
		int readCount;
		
		public FakeObjectChannel(List<? extends Object> readMsgs) {
			this.readMsgs = readMsgs;
		}

		@Override
		public void close() {
			
		}

		@Override
		public boolean isOpen() {
			return true;
		}

		@Override
		public Future<?> write(Object msg) {
			msgs.add(msg);
			ResultsFuture<?> result = new ResultsFuture<Void>();
			result.getResultsReceiver().receiveResults(null);
			return result;
		}
		
		@Override
		public Object read() throws IOException,
				ClassNotFoundException {
		    if (readCount >= readMsgs.size()) {
			return "";
		    }

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
		
		@Override
		public SocketAddress getRemoteAddress() {
			return null;
		}
		
		@Override
		public ObjectChannel createObjectChannel(SocketAddress address,
				boolean ssl) throws IOException, CommunicationException {
			return this;
		}
		
		@Override
		public int getSoTimeout() {
			return 1;
		}
		
	}

	@Test public void testHandshakeTimeout() throws Exception {
		SocketTimeoutException[] exs = new SocketTimeoutException[SocketServerInstanceImpl.HANDSHAKE_RETRIES];
		Arrays.fill(exs, new SocketTimeoutException());
		final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(exs));
		
		try {
			createInstance(channel);
			fail("Exception expected"); //$NON-NLS-1$
		} catch (IOException e) {
			
		}
	}

	private SocketServerInstanceImpl createInstance(ObjectChannelFactory channelFactory)
			throws CommunicationException, IOException {
		HostInfo info = new HostInfo("0.0.0.0", 1);
		SocketServerInstanceImpl ssii = new SocketServerInstanceImpl(info, 1);
		ssii.connect(channelFactory);
		return ssii;
	}
	
	@Test public void testSuccessfulHandshake() throws Exception {
		final FakeObjectChannel channel = new FakeObjectChannel(Arrays.asList(new Handshake(), new SocketTimeoutException()));
		
		SocketServerInstanceImpl instance = createInstance(channel);
		
		//no remote server is hooked up, so this will timeout
		ILogon logon = instance.getService(ILogon.class);
		try {
			logon.logon(new Properties());
			fail("Exception expected"); //$NON-NLS-1$
		} catch (TeiidComponentException e) {
			assertTrue(e.getCause().getCause() instanceof TimeoutException);
		}
	}
	
}
