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

package org.teiid.net.socket;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.logging.Logger;

import javax.net.ssl.*;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.jdbc.JDBCPlugin;



/**
 * This class provides some utility methods to create ssl sockets using the
 * keystores and trust stores. these are the properties required for the making the
 * ssl connection
 */
public class SocketUtil {
    private static Logger logger = Logger.getLogger(SocketUtil.class.getName());

    static final String TRUSTSTORE_PASSWORD = "org.teiid.ssl.trustStorePassword"; //$NON-NLS-1$
    public static final String TRUSTSTORE_FILENAME = "org.teiid.ssl.trustStore"; //$NON-NLS-1$
    static final String KEYSTORE_ALGORITHM = "org.teiid.ssl.algorithm"; //$NON-NLS-1$
    static final String PROTOCOL = "org.teiid.ssl.protocol"; //$NON-NLS-1$
    static final String KEYSTORE_TYPE = "org.teiid.ssl.keyStoreType"; //$NON-NLS-1$
    static final String KEYSTORE_PASSWORD = "org.teiid.ssl.keyStorePassword"; //$NON-NLS-1$
    static final String KEYSTORE_FILENAME = "org.teiid.ssl.keyStore"; //$NON-NLS-1$
    public static final String ALLOW_ANON = "org.teiid.ssl.allowAnon"; //$NON-NLS-1$
    static final String KEYSTORE_ALIAS = "org.teiid.ssl.keyAlias"; //$NON-NLS-1$
    static final String KEY_PASSWORD = "org.teiid.ssl.keyPassword"; //$NON-NLS-1$
    static final String TRUST_ALL = "org.teiid.ssl.trustAll"; //$NON-NLS-1$
    static final String CHECK_EXPIRED = "org.teiid.ssl.checkExpired"; //$NON-NLS-1$

    public static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$

    public static final String ANON_CIPHER_SUITE = "TLS_DH_anon_WITH_AES_128_CBC_SHA"; //$NON-NLS-1$
    public static final String DEFAULT_PROTOCOL = "TLSv1.2"; //$NON-NLS-1$

    private final static X509TrustManager[] TRUST_ALL_MANAGER = new X509TrustManager[] {new X509TrustManager() {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override
        public void checkServerTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {

        }

        @Override
        public void checkClientTrusted(X509Certificate[] arg0, String arg1)
                throws CertificateException {

        }
    }};

    public static class SSLSocketFactory {
        private boolean isAnon;
        private boolean warned;
        private javax.net.ssl.SSLSocketFactory factory;

        public SSLSocketFactory(SSLContext context, boolean isAnon) {
            this.factory = context.getSocketFactory();
            this.isAnon = isAnon;
        }

        public synchronized Socket getSocket(String host, int port) throws IOException {
            SSLSocket result = (SSLSocket)factory.createSocket(host, port);
            result.setUseClientMode(true);
            if (isAnon && !addCipherSuite(result, ANON_CIPHER_SUITE) && !warned) {
                warned = true;
                logger.warning(JDBCPlugin.Util.getString("SocketUtil.anon_not_available")); //$NON-NLS-1$
            }
            return result;
        }
    }

    public static SSLSocketFactory getSSLSocketFactory(Properties props) throws IOException, GeneralSecurityException{
        String keystore = props.getProperty(KEYSTORE_FILENAME);
        String keystorePassword = props.getProperty(KEYSTORE_PASSWORD);
        String keystoreType = props.getProperty(KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
        String keystoreProtocol = props.getProperty(PROTOCOL, DEFAULT_PROTOCOL);
        String keystoreAlgorithm = props.getProperty(KEYSTORE_ALGORITHM);
        String truststore = props.getProperty(TRUSTSTORE_FILENAME, keystore);
        String truststorePassword = props.getProperty(TRUSTSTORE_PASSWORD, keystorePassword);
        String keyAlias = props.getProperty(KEYSTORE_ALIAS);
        String keyPassword = props.getProperty(KEY_PASSWORD);

        boolean anon = PropertiesUtils.getBooleanProperty(props, ALLOW_ANON, true);
        boolean trustAll = PropertiesUtils.getBooleanProperty(props, TRUST_ALL, false);
        boolean checkExpired = PropertiesUtils.getBooleanProperty(props, CHECK_EXPIRED, false);

        SSLContext result = null;
        // 1) keystore != null = 2 way SSL (can define a separate truststore too)
        // 2) truststore != null = 1 way SSL (here we can define custom properties for truststore; useful when
        //    client like a appserver have to define multiple certs without importing
        //    all the certificates into one single certificate
        // 3) else = javax properties; this is default way to define the SSL anywhere.
        if (keystore != null || truststore != null || trustAll || checkExpired) {
            result = getSSLContext(keystore, keystorePassword, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol, keyAlias, keyPassword, trustAll, checkExpired);
        } else {
            result = SSLContext.getDefault();
        }
        //TODO: could allow a custom SSLSocketFactory to be plugged in like the pg jdbc driver
        return new SSLSocketFactory(result, anon);
    }

    public static boolean addCipherSuite(SSLSocket engine, String cipherSuite) {
        if (!Arrays.asList(engine.getSupportedCipherSuites()).contains(cipherSuite)) {
            return false;
        }

        String[] suites = engine.getEnabledCipherSuites();

        String[] newSuites = new String[suites.length + 1];
        System.arraycopy(suites, 0, newSuites, 0, suites.length);

        newSuites[suites.length] = cipherSuite;

        engine.setEnabledCipherSuites(newSuites);
        return true;
    }

    public static SSLContext getAnonSSLContext() throws IOException, GeneralSecurityException {
        return getSSLContext(null, null, null, null, null, null, DEFAULT_PROTOCOL, null, null, false, false);
    }

    public static SSLContext getSSLContext(KeyManager[] keyManagers, TrustManager[] trustManagers, String protocol)
            throws IOException, GeneralSecurityException {
        // Configure the SSL
        SSLContext sslc = SSLContext.getInstance(protocol);
        sslc.init(keyManagers, trustManagers, null);
        return sslc;
    }

    public static SSLContext getSSLContext(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol,
                                            String keyAlias,
                                            String keyPassword,
                                            boolean trustAll,
                                            boolean checkExpired) throws IOException, GeneralSecurityException {


        KeyManager[] keyManagers = getKeyManagers(keystore, password, algorithm, keystoreType, keyAlias, keyPassword);
        // Configure the Trust Store Manager
        TrustManager[] trustManagers = null;
        if (trustAll) {
            trustManagers = TRUST_ALL_MANAGER;
        } else {
            trustManagers = getTrustManagers(truststore, truststorePassword, algorithm, keystoreType, checkExpired);
        }

        // Configure the SSL
        SSLContext sslc = SSLContext.getInstance(protocol);
        sslc.init(keyManagers, trustManagers, null);
        return sslc;
    }

    private static TrustManager[] getCheckExpiredTrustManager(String algorithm,
            TrustManager[] trustManagers) throws NoSuchAlgorithmException,
            KeyStoreException {
        if (trustManagers == null) {
            //use the default
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
            tmf.init((KeyStore)null);
            trustManagers = tmf.getTrustManagers();
        }
        //this should always be the case, but we'll check
        if (trustManagers.length > 0 && trustManagers[0] instanceof X509TrustManager) {
            final X509TrustManager tm = (X509TrustManager)trustManagers[0];
            trustManagers[0] = new X509TrustManager() {

                @Override
                public X509Certificate[] getAcceptedIssuers() {
                    return tm.getAcceptedIssuers();
                }

                @Override
                public void checkServerTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                    tm.checkServerTrusted(chain, authType);
                    Date date = new Date();
                    for (X509Certificate cert : chain) {
                        if (cert.getNotBefore().after(date) || cert.getNotAfter().before(date)) {
                            throw new CertificateException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20038));
                        }
                    }
                }

                @Override
                public void checkClientTrusted(X509Certificate[] chain, String authType)
                        throws CertificateException {
                    tm.checkClientTrusted(chain, authType);
                    Date date = new Date();
                    for (X509Certificate cert : chain) {
                        if (cert.getNotBefore().after(date) || cert.getNotAfter().before(date)) {
                            throw new CertificateException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20038));
                        }
                    }
                }
            };
        }
        return trustManagers;
    }

    /**
     * Load any defined keystore file, by first looking in the classpath
     * then looking in the file system path.
     *
     * @param name - name of the keystore
     * @param password - password to load the keystore
     * @param type - type of the keystore
     * @return loaded keystore
     */
    public static KeyStore loadKeyStore(String name, String password, String type) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {

        // Check in the classpath
        InputStream stream = SocketUtil.class.getClassLoader().getResourceAsStream(name);
        if (stream == null) {
            try {
                stream = new FileInputStream(name);
            } catch (FileNotFoundException e) {
                IOException exception = new IOException(JDBCPlugin.Util.getString("SocketHelper.keystore_not_found", name)); //$NON-NLS-1$
                exception.initCause(e);
                throw exception;
            }
        }

        KeyStore ks = KeyStore.getInstance(type);
        try {
            ks.load(stream, password != null ? password.toCharArray() : null);
        } finally {
            stream.close();
        }
        return ks;
    }

    static class AliasAwareKeyManager extends X509ExtendedKeyManager {
        private X509KeyManager delegate;
        private String keyAlias;

        public AliasAwareKeyManager(X509KeyManager delegate, String alias) {
            this.delegate = delegate;
            this.keyAlias = alias;
        }

        @Override
        public String chooseClientAlias(String[] keyType, Principal[] issuers,Socket socket) {
            return keyAlias;
        }

        @Override
        public String chooseServerAlias(String keyType, Principal[] issuers, Socket socket) {
            return keyAlias;
        }

        @Override
        public X509Certificate[] getCertificateChain(String alias) {
            return delegate.getCertificateChain(alias);
        }

        @Override
        public String[] getClientAliases(String keyType, Principal[] issuers) {
            return delegate.getClientAliases(keyType, issuers);
        }

        @Override
        public PrivateKey getPrivateKey(String alias) {
            return delegate.getPrivateKey(alias);
        }

        @Override
        public String[] getServerAliases(String keyType, Principal[] issuers) {
            return delegate.getServerAliases(keyType, issuers);
        }

        @Override
        public String chooseEngineClientAlias(String[] keyType,
                Principal[] issuers, SSLEngine engine) {
            return keyAlias;
        }

        @Override
        public String chooseEngineServerAlias(String keyType,
                Principal[] issuers, SSLEngine engine) {
            return keyAlias;
        }
    }

    public static KeyManager[] getKeyManagers(String keystore,
            String password,
            String algorithm,
            String keystoreType,
            String keyAlias,
            String keyPassword
            ) throws IOException, GeneralSecurityException {
        if (algorithm == null) {
            algorithm = KeyManagerFactory.getDefaultAlgorithm();
        }
        // Configure the Keystore Manager
        KeyManager[] keyManagers = null;
        if (keystore != null) {
            KeyStore ks = loadKeyStore(keystore, password, keystoreType);
            if (ks != null) {

                KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
                if (keyPassword == null) {
                    keyPassword = password;
                }
                kmf.init(ks, keyPassword != null ? keyPassword.toCharArray() : null);
                keyManagers = kmf.getKeyManagers();
                if (keyAlias != null) {
                    if (!ks.isKeyEntry(keyAlias)) {
                        throw new GeneralSecurityException(JDBCPlugin.Util.getString("alias_no_key_entry", keyAlias)); //$NON-NLS-1$
                    }
                    if (DEFAULT_KEYSTORE_TYPE.equals(keystoreType)) {
                        keyAlias = keyAlias.toLowerCase(Locale.ENGLISH);
                    }
                    for(int i=0; i < keyManagers.length; i++) {
                        if (keyManagers[i] instanceof X509KeyManager) {
                            keyManagers[i] = new AliasAwareKeyManager((X509KeyManager)keyManagers[i], keyAlias);
                        }
                    }
                }
            }
        }
        return keyManagers;
    }

    public static TrustManager[] getTrustManagers(String truststore, String truststorePassword, String algorithm,
            String keystoreType, boolean checkExpired) throws IOException, GeneralSecurityException {

        if (algorithm == null) {
            algorithm = KeyManagerFactory.getDefaultAlgorithm();
        }

        TrustManager[] trustManagers = null;
        if (truststore != null) {
            KeyStore ks = loadKeyStore(truststore, truststorePassword, keystoreType);
            if (ks != null) {
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
                tmf.init(ks);
                trustManagers = tmf.getTrustManagers();
            }
        }

        if (checkExpired) {
            trustManagers = getCheckExpiredTrustManager(algorithm, trustManagers);
        }
        return trustManagers;
    }

    public static TrustManager[] getTrustAllManagers() {
        return TRUST_ALL_MANAGER;
    }
}
