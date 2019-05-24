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
import org.teiid.core.TeiidComponentException;
import org.teiid.dqp.service.SessionService;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.HostInfo;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.net.socket.SocketServerConnectionFactory;
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
        p.setProperty(TeiidURL.CONNECTION.AUTO_FAILOVER, Boolean.TRUE.toString());
        if (sscf == null) {
            sscf = new SocketServerConnectionFactory();
            sscf.initialize(socketConfig);
        }

        return sscf.getConnection(p);
    }

    private SocketListener createListener(InetSocketAddress address, SSLConfiguration config) {
        ClientServiceRegistryImpl server = new ClientServiceRegistryImpl() {
            @Override
            public ClassLoader getCallerClassloader() {
                return getClass().getClassLoader();
            }
        };
        SessionService ss = mock(SessionService.class);
        server.registerClientService(ILogon.class, new LogonImpl(ss, "fakeCluster") { //$NON-NLS-1$
            @Override
            public LogonResult logon(Properties connProps)
                    throws LogonException {
                logonAttempts++;
                return new LogonResult(new SessionToken("dummy"), "x", "z");
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
        return new SocketListener(new InetSocketAddress(address.getAddress().getHostAddress(),address.getPort()), 0, 0, 2, config, server, BufferManagerFactory.getStandaloneBufferManager());
    }

    @Test public void testFailover() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        Properties p = new Properties();
        SocketServerConnection conn = helpEstablishConnection(false, config, p);
        assertTrue(conn.isOpen(1000));
        assertEquals(1, logonAttempts);
        //restart the second instance now that we know the connection was made to the first
        listener1 = createListener(new InetSocketAddress(addr.getAddress(), listener1.getPort()), config);
        listener.stop().await();
        conn.isOpen(1000); //there is a chance this call can fail
        assertTrue(conn.isOpen(1000));
        assertEquals(2, logonAttempts);
        listener1.stop().await();
        //both instances are down
        assertFalse(conn.isOpen(1000));
        //bring the first back up
        listener = createListener(new InetSocketAddress(addr.getAddress(), listener.getPort()), config);
        assertTrue(conn.isOpen(1000));
        assertEquals(3, logonAttempts);
        conn.close();
    }

}
