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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.client.security.ILogon;
import org.teiid.client.util.ResultsFuture;
import org.teiid.client.util.ResultsFuture.CompletionListener;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.MemoryStorageManager;
import org.teiid.core.crypto.NullCryptor;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.net.socket.SocketServerConnection;
import org.teiid.net.socket.SocketServerConnectionFactory;
import org.teiid.net.socket.SocketUtil;
import org.teiid.services.SessionServiceImpl;
import org.teiid.transport.SSLConfiguration.ClientAuth;
import org.teiid.transport.TestSocketRemoting.FakeService;


@SuppressWarnings("nls")
public class TestCommSockets {

    SocketListener listener;
    private SocketServerConnectionFactory sscf;
    private InetSocketAddress addr;
    private MemoryStorageManager storageManager;
    private SessionServiceImpl service;
    private AuthenticationType authType = AuthenticationType.USERPASSWORD;

    @Before public void setUp() {
        addr = new InetSocketAddress(0);
    }

    @After public void tearDown() throws Exception {
        if (listener != null) {
            listener.stop();
        }
        authType = AuthenticationType.USERPASSWORD;
    }

    @Test(expected=CommunicationException.class) public void testFailedConnect() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        listener = new SocketListener(addr,1024, 1024, 1, config, null, BufferManagerFactory.getStandaloneBufferManager());

        Properties p = new Properties();
        String url = new TeiidURL(addr.getHostName(), listener.getPort() - 1, false).getAppServerURL();
        p.setProperty(TeiidURL.CONNECTION.SERVER_URL, url); //wrong port
        SocketServerConnectionFactory.getInstance().getConnection(p);
    }

    @Test public void testConnectWithoutPooling() throws Exception {
        Properties p = new Properties();
        SocketServerConnection conn = helpEstablishConnection(false, new SSLConfiguration(), p);
        SocketListenerStats stats = listener.getStats();
        assertEquals(2, stats.objectsRead); // handshake response, logon,
        assertEquals(1, stats.sockets);
        conn.close();
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

    @Test public void testLobs() throws Exception {
        SocketServerConnection conn = helpEstablishConnection(false);
        FakeService fs = conn.getService(FakeService.class);
        assertEquals(150, fs.lobMethod(new ByteArrayInputStream(new byte[100]), new StringReader(new String(new char[50]))));
        assertEquals(2, storageManager.getCreated());
        assertEquals(2, storageManager.getRemoved());
        assertEquals(0, fs.lobMethod(new ByteArrayInputStream(new byte[0]), new StringReader(new String(new char[0]))));
        assertEquals(4, storageManager.getCreated());
        assertEquals(4, storageManager.getRemoved());
        assertEquals((1 << 17) + 50, fs.lobMethod(new ByteArrayInputStream(new byte[1 << 17]), new StringReader(new String(new char[50]))));
        assertEquals(6, storageManager.getCreated());
        assertEquals(6, storageManager.getRemoved());
    }

    @Test public void testServerRemoteStreaming() throws Exception {
        SocketServerConnection conn = helpEstablishConnection(false);
        FakeService fs = conn.getService(FakeService.class);
        assertEquals("hello world", ObjectConverterUtil.convertToString(fs.getReader()));
        assertTrue(Serializable.class.isAssignableFrom(fs.getReader().getClass()));
    }

    @Test public void testConnectWithoutClientEncryption() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.DISABLED);
        SocketServerConnection conn = helpEstablishConnection(false, config, new Properties());
        assertTrue(conn.selectServerInstance().getCryptor() instanceof NullCryptor);
        conn.close();
    }

    private SocketServerConnection helpEstablishConnection(boolean secure) throws CommunicationException, ConnectionException {
        return helpEstablishConnection(secure, new SSLConfiguration(), new Properties());
    }

    private SocketServerConnection helpEstablishConnection(boolean clientSecure, SSLConfiguration config, Properties socketConfig) throws CommunicationException,
            ConnectionException {
        if (listener == null) {
            ClientServiceRegistryImpl server = new ClientServiceRegistryImpl() {
                @Override
                public ClassLoader getCallerClassloader() {
                    return getClass().getClassLoader();
                }
            };
            service = new SessionServiceImpl();
            service.setAuthenticationType(authType);
            server.registerClientService(ILogon.class, new LogonImpl(service, "fakeCluster"), null);
            server.registerClientService(FakeService.class, new TestSocketRemoting.FakeServiceImpl(), null);
            storageManager = new MemoryStorageManager();
            listener = new SocketListener(addr, 0, 0, 2, config, server, storageManager);

            SocketListenerStats stats = listener.getStats();
            assertEquals(0, stats.maxSockets);
            assertEquals(0, stats.objectsRead);
            assertEquals(0, stats.objectsWritten);
            assertEquals(0, stats.sockets);
        }

        Properties p = new Properties(socketConfig);
        String url = new TeiidURL(addr.getHostName(), listener.getPort(), clientSecure).getAppServerURL();
        p.setProperty(TeiidURL.CONNECTION.SERVER_URL, url);
        p.setProperty(TeiidURL.CONNECTION.APP_NAME, "test");
        if (sscf == null) {
            sscf = new SocketServerConnectionFactory();
            sscf.initialize(socketConfig);
        }
        return sscf.getConnection(p);
    }

    @Test public void testSSLConnectWithNonSSLServer() throws Exception {
        //first make a non-ssl connection to ensure that it's not reused
        SocketServerConnection conn = helpEstablishConnection(false);
        conn.close();
        try {
            helpEstablishConnection(true);
            fail("exception expected"); //$NON-NLS-1$
        } catch (CommunicationException e) {

        }
    }

    @Test public void testAnonSSLConnect() throws Exception {
        SSLContext context = SSLContext.getDefault();
        String[] ciphers = context.getServerSocketFactory().getSupportedCipherSuites();
        if (!Arrays.asList(ciphers).contains(SocketUtil.ANON_CIPHER_SUITE)) {
            //Cannot test anon if no cipher suite is available
            return;
        }

        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setEnabledCipherSuites("x"); //ensure that this cipher suite is not used
        config.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
        Properties p = new Properties();
        helpEstablishConnection(true, config, p);
        SocketServerConnection conn = helpEstablishConnection(true, config, p);
        conn.close();

        try {
            helpEstablishConnection(false, config, p);
            fail();
        } catch (CommunicationException e) {

        }
    }

    @Test(expected=CommunicationException.class) public void testNonAnonSSLConnectWithSSLServer() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.ANONYMOUS);
        Properties p = new Properties();
        p.setProperty(SocketUtil.ALLOW_ANON, Boolean.FALSE.toString());
        helpEstablishConnection(true, config, p);
    }

    @Test(expected=CommunicationException.class) public void testOnewayFails() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.ONEWAY);
        Properties p = new Properties();
        helpEstablishConnection(true, config, p);
        //TODO: we may want to clarify the server exception in this case, which
        //is just that there are no cipher suites in common
    }

    /**
     * shows one-way auth with a key alias/password
     */
    @Test public void testSSLSelfSigned() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.ONEWAY);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setKeystorePassword("password");
        config.setKeystoreKeyPassword("changeit");
        config.setKeystoreKeyAlias("selfsigned");
        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.trustStorePassword", "password");
        helpEstablishConnection(true, config, p);
    }

    @Test public void testSSLSelfSignedTrustAll() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.ONEWAY);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setKeystorePassword("password");
        config.setKeystoreKeyPassword("changeit");
        config.setKeystoreKeyAlias("selfsigned");
        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustAll", "true");
        helpEstablishConnection(true, config, p);
    }

    @Test public void testTwoWaySSLSelfSigned() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.TWOWAY);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setKeystorePassword("password");
        config.setKeystoreKeyPassword("changeit");
        config.setKeystoreKeyAlias("selfsigned");
        config.setTruststoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setTruststorePassword("password");
        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.trustStorePassword", "password");
        p.setProperty("org.teiid.ssl.keyStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.keyStorePassword", "password");
        p.setProperty("org.teiid.ssl.keyPassword", "changeit");
        helpEstablishConnection(true, config, p);
    }

    @Test(expected = ConnectionException.class) public void testTwoWayWtihAuthFails() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(ClientAuth.WANT);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setKeystorePassword("password");
        config.setKeystoreKeyPassword("changeit");
        config.setKeystoreKeyAlias("selfsigned");
        config.setTruststoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setTruststorePassword("password");
        authType = AuthenticationType.SSL;

        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.trustStorePassword", "password");
        //1-way will fail
        helpEstablishConnection(true, config, p);
    }

    @Test public void testTwoWayWithAuth() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(ClientAuth.WANT);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setKeystorePassword("password");
        config.setKeystoreKeyPassword("changeit");
        config.setKeystoreKeyAlias("selfsigned");
        config.setTruststoreFilename(UnitTestUtil.getTestDataPath() + "/keystore.jks");
        config.setTruststorePassword("password");
        authType = AuthenticationType.SSL;

        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.trustStorePassword", "password");

        p.setProperty("org.teiid.ssl.keyStore", UnitTestUtil.getTestDataPath() + "/keystore.jks");
        p.setProperty("org.teiid.ssl.keyStorePassword", "password");
        p.setProperty("org.teiid.ssl.keyPassword", "changeit");

        helpEstablishConnection(true, config, p);
    }

    @Test public void testSelectNewInstanceWithoutPooling() throws Exception {
        Properties p = new Properties();
        SSLConfiguration config = new SSLConfiguration();
        SocketServerConnection conn = helpEstablishConnection(false, config, p);
        SocketListenerStats stats = listener.getStats();
        assertEquals(2, stats.objectsRead); // handshake response, logon,
        assertEquals(1, stats.sockets);
        assertEquals(1, this.service.getActiveSessionsCount());
        ServerConnection conn2 = helpEstablishConnection(false, config, p);
        assertEquals(2, this.service.getActiveSessionsCount());
        conn.selectServerInstance();
        assertEquals(2, this.service.getActiveSessionsCount());
        assertTrue(conn.isOpen(10000));
        stats = listener.getStats();
        assertEquals(5, stats.objectsRead); // (ping (pool test), assert identityx2, ping (isOpen))x2
        assertEquals(2, stats.sockets);
        conn.close();
        conn2.close();
    }

    @Test public void testEnableCipherSuites() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setEnabledCipherSuites("x,y,z");
        assertArrayEquals(new String[] {"x","y","z"}, config.getEnabledCipherSuitesAsArray());
    }

    @Test public void testAnonSSLMode() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode("enabled");
        assertFalse(config.isClientEncryptionEnabled());
        assertTrue(config.isSslEnabled());
        config.setMode("login");
        assertTrue(config.isClientEncryptionEnabled());
    }

    @Test(expected=CommunicationException.class) public void testCheckExpired() throws Exception {
        SSLConfiguration config = new SSLConfiguration();
        config.setMode(SSLConfiguration.ENABLED);
        config.setAuthenticationMode(SSLConfiguration.ONEWAY);
        config.setKeystoreFilename(UnitTestUtil.getTestDataPath() + "/TEIID-4080/keystore_server_root_expired.jks");
        config.setKeystorePassword("keystorepswd");

        Properties p = new Properties();
        p.setProperty("org.teiid.ssl.trustStore", UnitTestUtil.getTestDataPath() + "/TEIID-4080/truststore.jks");
        p.setProperty("org.teiid.ssl.trustStorePassword", "truststorepswd");
        p.setProperty("org.teiid.ssl.checkExpired", "true");
        helpEstablishConnection(true, config, p);
    }

    @Test public void testAutoFailoverPing() throws Exception {
        Properties p = new Properties();
        p.setProperty(TeiidURL.CONNECTION.AUTO_FAILOVER, "true");
        p.setProperty("org.teiid.sockets.synchronousttl", "20000");
        SocketServerConnection conn = helpEstablishConnection(false, new SSLConfiguration(), p);
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        Future<?> future = exec.submit(new Runnable() {

            @Override
            public void run() {
                final FakeService fs = conn.getService(FakeService.class);
                ResultsFuture<Integer> f = fs.delayedAsynchResult();
                f.addCompletionListener(new CompletionListener<Integer>() {
                    @Override
                    public void onCompletion(ResultsFuture<Integer> future) {
                        fs.asynchResult(); //potentially recurrent;
                    }
                });
                try {
                    f.get();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        future.get(19, TimeUnit.SECONDS);
    }

    @Test public void testSocketConfig() throws Exception {
        assertEquals(240001l, SocketServerConnectionFactory.getInstance().getSynchronousTtl());
    }

}
