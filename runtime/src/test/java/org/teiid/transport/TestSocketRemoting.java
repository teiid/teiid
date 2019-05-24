/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.transport;

import static org.junit.Assert.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.Serializable;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.teiid.client.DQP;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsReceiver;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.core.crypto.Cryptor;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.core.util.ApplicationInfo;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.Message;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.net.socket.SocketServerInstance;
import org.teiid.net.socket.SocketServerInstanceFactory;
import org.teiid.net.socket.SocketServerInstanceImpl;
import org.teiid.net.socket.UrlServerDiscovery;

@SuppressWarnings("nls")
public class TestSocketRemoting {

    public interface FakeService {

        ResultsFuture<Integer> asynchResult();

        ResultsFuture<Integer> delayedAsynchResult();

        String exceptionMethod() throws TeiidProcessingException;

        int lobMethod(InputStream is, Reader r) throws IOException;

        Reader getReader() throws IOException;

    }

    static class FakeServiceImpl implements FakeService {

        public ResultsFuture<Integer> asynchResult() {
            ResultsFuture<Integer> result = new ResultsFuture<Integer>();
            result.getResultsReceiver().receiveResults(new Integer(5));
            return result;
        }

        public String exceptionMethod() throws TeiidProcessingException {
            throw new TeiidProcessingException();
        }

        @Override
        public int lobMethod(InputStream is, Reader r) throws IOException {
            return ObjectConverterUtil.convertToByteArray(is).length + ObjectConverterUtil.convertToString(r).length();
        }

        @Override
        public Reader getReader() throws IOException {
            return new StringReader("hello world"); //$NON-NLS-1$
        }

        @Override
        public ResultsFuture<Integer> delayedAsynchResult() {
            ResultsFuture<Integer> result = new ResultsFuture<Integer>();
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
            }
            result.getResultsReceiver().receiveResults(new Integer(5));
            return result;
        }

    }

    private static class FakeClientServerInstance extends SocketServerInstanceImpl implements ClientInstance {

        ClientServiceRegistryImpl server;
        private ResultsReceiver<Object> listener;

        public FakeClientServerInstance(ClientServiceRegistryImpl server) throws UnknownHostException {
            super(new HostInfo("foo", new InetSocketAddress(InetAddress.getLocalHost(), 1)), 1000, 1000);
            this.server = server;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void send(Message message, ResultsReceiver<Object> l,
                Serializable messageKey) throws CommunicationException,
                InterruptedException {
            ServerWorkItem workItem = new ServerWorkItem(this, messageKey, message, server);
            this.listener = l;
            workItem.run();
        }

        @Override
        public void shutdown() {

        }

        @Override
        public Cryptor getCryptor() {
            return new NullCryptor();
        }

        public DQPWorkContext getWorkContext() {
            return new DQPWorkContext();
        }

        public void send(Message message, Serializable messageKey) {
            this.listener.receiveResults(message.getContents());
        }

        @Override
        public String getServerVersion() {
            return ApplicationInfo.getInstance().getReleaseNumber();
        }

    }

    /**
     * No server was supplied, will throw an NPE under the covers
     */
    @Test
    public void testUnckedException() throws Exception {
        FakeClientServerInstance serverInstance = new FakeClientServerInstance(null);
        try {
            createFakeConnection(serverInstance);
            fail("expected exception"); //$NON-NLS-1$
        } catch (CommunicationException e) {
            assertEquals("TEIID20018 Unable to find a component used authenticate on to Teiid", e.getMessage()); //$NON-NLS-1$
        } catch(NullPointerException npe) {
        }
    }

    @Test
    public void testMethodInvocation() throws Exception {
        ClientServiceRegistryImpl csr = new ClientServiceRegistryImpl() {
            @Override
            public ClassLoader getCallerClassloader() {
                return getClass().getClassLoader();
            }
        };
        csr.registerClientService(ILogon.class, new ILogon() {

                public ResultsFuture<?> logoff()
                        throws InvalidSessionException {
                    ResultsFuture<?> result = new ResultsFuture<Void>();
                    result.getResultsReceiver().exceptionOccurred(new TeiidComponentException("some exception")); //$NON-NLS-1$
                    return result;
                }

                public LogonResult logon(Properties connectionProperties)
                        throws LogonException, TeiidComponentException {
                    return new LogonResult();
                }

                // tests asynch where we don't care about the result
                public ResultsFuture<?> ping() throws InvalidSessionException,
                        TeiidComponentException {
                    return null;
                }

                @Override
                public ResultsFuture<?> ping(Collection<String> sessions)
                    throws TeiidComponentException, CommunicationException {
                    return null;
                }

                @Override
                public void assertIdentity(SessionToken sessionId)
                    throws InvalidSessionException,
                    TeiidComponentException {
                }

                @Override
                public LogonResult neogitiateGssLogin(
                        Properties connectionProperties, byte[] serviceToken, boolean createSession)
                        throws LogonException {
                    return null;
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
            assertTrue(e.getCause() instanceof TeiidComponentException);
        }
        FakeService service = connection.getService(FakeService.class);
        Future<Integer> asynchInteger = service.asynchResult();
        assertEquals(new Integer(5), asynchInteger.get(0, TimeUnit.MILLISECONDS));
        try {
            service.exceptionMethod();
            fail("exception expected"); //$NON-NLS-1$
        } catch (TeiidProcessingException e) {

        }
        DQP dqp = connection.getService(DQP.class);
        try {
            ResultsFuture<?> future = dqp.begin();
            future.get();
            fail("exception expected"); //$NON-NLS-1$
        } catch (Exception e) {
            assertTrue(e.getMessage().indexOf("Component not found:") != -1); //$NON-NLS-1$
        }
    }

    private SocketServerConnection createFakeConnection(
            final FakeClientServerInstance serverInstance)
            throws CommunicationException, ConnectionException {
        SocketServerConnection connection = new SocketServerConnection(new SocketServerInstanceFactory() {

            @Override
            public SocketServerInstance getServerInstance(HostInfo info)
                    throws CommunicationException, IOException {
                return serverInstance;
            }

            @Override
            public String resolveHostname(InetAddress addr) {
                return null;
            }

        }, false, new UrlServerDiscovery(new TeiidURL("0.0.0.0", 1, false)), new Properties()); //$NON-NLS-1$
        return connection;
    }

}
