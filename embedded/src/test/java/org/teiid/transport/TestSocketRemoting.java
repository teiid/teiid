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

package org.teiid.transport;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.transport.ClientInstance;
import org.teiid.transport.ServerWorkItem;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.HostInfo;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.ClientServiceRegistry;
import com.metamatrix.common.comm.api.Message;
import com.metamatrix.common.comm.api.ResultsReceiver;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnection;
import com.metamatrix.common.comm.platform.socket.client.SocketServerInstance;
import com.metamatrix.common.comm.platform.socket.client.SocketServerInstanceFactory;
import com.metamatrix.common.comm.platform.socket.client.SocketServerInstanceImpl;
import com.metamatrix.common.comm.platform.socket.client.UrlServerDiscovery;
import com.metamatrix.common.util.crypto.Cryptor;
import com.metamatrix.common.util.crypto.NullCryptor;
import com.metamatrix.common.xa.XATransactionException;
import com.metamatrix.core.util.SimpleMock;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.dqp.client.ResultsFuture;
import com.metamatrix.platform.security.api.ILogon;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.SessionToken;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;

public class TestSocketRemoting extends TestCase {
	
	public interface FakeService {
		
		ResultsFuture<Integer> asynchResult();
		
		String exceptionMethod() throws MetaMatrixProcessingException;
		
	}
	
	private static class FakeServiceImpl implements FakeService {

		public ResultsFuture<Integer> asynchResult() {
			ResultsFuture<Integer> result = new ResultsFuture<Integer>();
			result.getResultsReceiver().receiveResults(new Integer(5));
			return result;
		}

		public String exceptionMethod() throws MetaMatrixProcessingException {
			throw new MetaMatrixProcessingException();
		}
		
	}
	
	private static class FakeClientServerInstance extends SocketServerInstanceImpl implements ClientInstance {
		
		ClientServiceRegistry clientServiceRegistry;
		private ResultsReceiver<Object> listener;

		public FakeClientServerInstance(ClientServiceRegistry clientServiceRegistry) {
			super();
			this.clientServiceRegistry = clientServiceRegistry;
		}

		public HostInfo getHostInfo() {
			return new HostInfo("fake", 1); //$NON-NLS-1$
		}

		public boolean isOpen() {
			return true;
		}
		
		@Override
		public void send(Message message, ResultsReceiver<Object> listener,
				Serializable messageKey) throws CommunicationException,
				InterruptedException {
			ServerWorkItem workItem = new ServerWorkItem(this, messageKey, message, clientServiceRegistry, SimpleMock.createSimpleMock(SessionServiceInterface.class));
			this.listener = listener;
			workItem.run();
		}

		public void shutdown() {
			
		}

		public Cryptor getCryptor() {
			return new NullCryptor();
		}

		public DQPWorkContext getWorkContext() {
			return new DQPWorkContext();
		}

		public void send(Message message, Serializable messageKey) {
			this.listener.receiveResults(message.getContents());
		}
		
	}
	
	/**
	 * No server was supplied, will throw an NPE under the covers
	 */
	public void testUnckedException() throws Exception {
		FakeClientServerInstance serverInstance = new FakeClientServerInstance(null);
		try {
			createFakeConnection(serverInstance);
			fail("expected exception"); //$NON-NLS-1$
		} catch (CommunicationException e) {
			assertEquals("Unable to find a component used in logging on to MetaMatrix", e.getMessage()); //$NON-NLS-1$
		}
	}
	
	public void testMethodInvocation() throws Exception {
		ClientServiceRegistry csr = new ClientServiceRegistry();
		csr.registerClientService(ILogon.class, new ILogon() {

				public ResultsFuture<?> logoff()
						throws InvalidSessionException,
						MetaMatrixComponentException {
					ResultsFuture<?> result = new ResultsFuture<Void>();
					result.getResultsReceiver().exceptionOccurred(new MetaMatrixComponentException("some exception")); //$NON-NLS-1$
					return result;
				}

				public LogonResult logon(Properties connectionProperties)
						throws LogonException, MetaMatrixComponentException {
					return new LogonResult();
				}

				// tests asynch where we don't care about the result
				public ResultsFuture<?> ping() throws InvalidSessionException,
						MetaMatrixComponentException {
					return null;
				}
				
				@Override
				public void assertIdentity(SessionToken sessionId)
					throws InvalidSessionException,
					MetaMatrixComponentException {
				}

			}, "foo"); //$NON-NLS-1$
		csr.registerClientService(FakeService.class, new FakeServiceImpl(), "foo"); //$NON-NLS-1$
		final FakeClientServerInstance serverInstance = new FakeClientServerInstance(csr);
		SocketServerConnection connection = createFakeConnection(serverInstance);
		ILogon logon = connection.getService(ILogon.class);
		Future<?> result = logon.ping();
		assertNull(result.get(0, TimeUnit.MILLISECONDS));
		result = logon.logoff();
		try {
			result.get(0, TimeUnit.MICROSECONDS);
			fail("exception expected"); //$NON-NLS-1$
		} catch (ExecutionException e) {
			assertTrue(e.getCause() instanceof MetaMatrixComponentException);
		}
		FakeService service = connection.getService(FakeService.class);
		Future<Integer> asynchInteger = service.asynchResult();
		assertEquals(new Integer(5), asynchInteger.get(0, TimeUnit.MILLISECONDS));
		try {
			service.exceptionMethod();
			fail("exception expected"); //$NON-NLS-1$
		} catch (MetaMatrixProcessingException e) {
			
		}
		ClientSideDQP dqp = connection.getService(ClientSideDQP.class);
		try {
			dqp.begin();
			fail("exception expected"); //$NON-NLS-1$
		} catch (XATransactionException e) {
			e.printStackTrace();
			assertEquals("Component not found: com.metamatrix.dqp.client.ClientSideDQP", e.getMessage()); //$NON-NLS-1$
		}
	}

	private SocketServerConnection createFakeConnection(
			final FakeClientServerInstance serverInstance)
			throws CommunicationException, ConnectionException {
		SocketServerConnection connection = new SocketServerConnection(new SocketServerInstanceFactory() {
		
			@Override
			public SocketServerInstance getServerInstance(HostInfo info,
					boolean ssl) throws CommunicationException, IOException {
				return serverInstance;
			}
			
		}, false, new UrlServerDiscovery(new MMURL("foo", 1, false)), new Properties(), null); //$NON-NLS-1$
		return connection;
	}
	
}
