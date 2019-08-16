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

/**
 *
 */
package org.teiid.net.socket;

import static org.junit.Assert.*;

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
            return new LogonResult(new SessionToken(1, connectionProperties.getProperty(TeiidURL.CONNECTION.USER_NAME, "fooUser")), "foo", "fake"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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
     * connection process.
     * @throws Throwable
     *
     * @since Westport
     */
    @Test public void testSocketServerConnection_PropertiesClientHost() throws Throwable {
        Properties p = new Properties();

        SocketServerConnection.updateConnectionProperties(p, InetAddress.getLocalHost(), true, null);

        assertTrue(p.containsKey(TeiidURL.CONNECTION.CLIENT_HOSTNAME));
        assertTrue(p.containsKey(TeiidURL.CONNECTION.CLIENT_IP_ADDRESS));
    }

    @Test public void testLogonFailsWithMultipleHosts() throws Exception {
        Properties p = new Properties();
        SocketServerInstanceFactory instanceFactory = Mockito.mock(SocketServerInstanceFactory.class);
        Mockito.stub(instanceFactory.getServerInstance((HostInfo)Mockito.anyObject())).toThrow(new SingleInstanceCommunicationException());
        UrlServerDiscovery discovery = new UrlServerDiscovery(new TeiidURL("mm://host1:1,host2:2")); //$NON-NLS-1$
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
        UrlServerDiscovery discovery = new UrlServerDiscovery(new TeiidURL(hostInfo.getHostName(), hostInfo.getPortNumber(), false));
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
            public String resolveHostname(InetAddress addr) {
                return null;
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
