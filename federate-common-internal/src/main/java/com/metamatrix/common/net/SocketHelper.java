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

package com.metamatrix.common.net;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.common.util.crypto.CryptoException;
import com.metamatrix.common.util.crypto.CryptoUtil;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.Assertion;


/**
 * The SSL configuration is separated into internal (host controller/RMI) and external
 * (client and console socket) connections.
 * 
 * @since 4.2
 */
public class SocketHelper {

    private static final String SSL_ENABLED       = "metamatrix.encryption.secure.sockets"; //$NON-NLS-1$
    
    private static final String INTERNAL_SSL_ENABLED = "metamatrix.encryption.internal.secure.sockets"; //$NON-NLS-1$
    private static final String INTERNAL_ENCRYPT_ALL = "metamatrix.encryption.internal.encryptAll"; //$NON-NLS-1$
    private static final String INTERANL_AUTHENTICATION_MODE = "metamatrix.encryption.internal.authenticationMode"; //$NON-NLS-1$
    
    private static final String KEYSTORE_FILENAME = "com.metamatrix.ssl.keystore.filename"; //$NON-NLS-1$
    private static final String KEYSTORE_PASSWORD = "com.metamatrix.ssl.keystore.Password"; //$NON-NLS-1$
    private static final String KEYSTORE_TYPE     = "com.metamatrix.ssl.keystoretype"; //$NON-NLS-1$
    private static final String SSL_PROTOCOL      = "com.metamatrix.ssl.protocol"; //$NON-NLS-1$
    private static final String KEY_MANAGER_ALGORITHM = "com.metamatrix.ssl.keymanagementalgorithm"; //$NON-NLS-1$
    
    private static final String TRUSTSTORE_FILENAME = "com.metamatrix.ssl.truststore.filename"; //$NON-NLS-1$
    private static final String TRUSTSTORE_PASSWORD = "com.metamatrix.ssl.truststore.Password"; //$NON-NLS-1$
//    private static String VALIDATE_HOSTS      = "com.metamatrix.ssl.validateHosts"; //$NON-NLS-1$
//    private static String ALLOW_EXPIRED_CERTS = "com.metamatrix.ssl.allowExpiredCerts"; //$NON-NLS-1$
    private static final String AUTHENTICATION_MODE         = "com.metamatrix.ssl.authenticationMode"; //$NON-NLS-1$
    public static final String ONEWAY = "1-way"; //$NON-NLS-1$ - one way is the default
    public static final String TWOWAY = "2-way"; //$NON-NLS-1$
    public static final String ANONYMOUS = "anonymous"; //$NON-NLS-1$

    private static final String DEFAULT_SSL_PROTOCOL = "SSLv3"; //$NON-NLS-1$
    private static final String DEFAULT_KEY_MANAGER_ALGORITHM = "SunX509"; //$NON-NLS-1$
    private static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    public static final String ANON_CIPHER_SUITE = "TLS_DH_anon_WITH_AES_128_CBC_SHA"; //$NON-NLS-1$
    public static final String ANON_PROTOCOL = "TLS"; //$NON-NLS-1$
    
    public static final String UNENCRYPTED_CIPHER_SUITE = "SSL_RSA_WITH_NULL_SHA"; //$NON-NLS-1$
    
    static boolean initialized = false;
    
    /*
     * External SSL resource settings
     */
    static boolean ssl_enabled = false;
    static String sslProtocol;
    static String keyManagerFactoryAlgorithm;
    static String keyStoreType;
    static String keyStoreFileName;
    static String keyStorePassword;
    static String trustStoreFileName;
    static String trustStorePassword;
    static String authenticationMode;
    
    /*
     * Internal SSL settings 
     */
    static boolean internal_ssl_enabled;
    static boolean internal_encrypt_all;
    static String internal_authenticationMode;

    /*
     * Client encryption property.  This may belong somewhere else
     */
    static boolean client_encryption_enabled = false;
    
    synchronized static void initProperties() {

        if (initialized) {
            return;
        }

        String server_val = CurrentConfiguration.getProperty(SSL_ENABLED);       
        ssl_enabled = Boolean.valueOf(server_val).booleanValue(); 
        
        internal_ssl_enabled = Boolean.valueOf(CurrentConfiguration.getProperty(INTERNAL_SSL_ENABLED, Boolean.TRUE.toString())).booleanValue();
        internal_encrypt_all = Boolean.valueOf(CurrentConfiguration.getProperty(INTERNAL_ENCRYPT_ALL, Boolean.FALSE.toString())).booleanValue();
        internal_authenticationMode = CurrentConfiguration.getProperty(INTERANL_AUTHENTICATION_MODE, TWOWAY);
        
        String clientEncrypt = CurrentConfiguration.getProperty(CommonPropertyNames.CLIENT_ENCRYPTION_ENABLED, Boolean.TRUE.toString());
        client_encryption_enabled = Boolean.valueOf(clientEncrypt).booleanValue();
        
        try {
            Properties serverProps = CurrentConfiguration.getResourceProperties(ResourceNames.SSL);

            keyStoreFileName = serverProps.getProperty(KEYSTORE_FILENAME);
            try {
                keyStorePassword = CryptoUtil.stringDecrypt(serverProps.getProperty(KEYSTORE_PASSWORD, "")); //$NON-NLS-1$
            } catch (CryptoException err) {
                throw new MetaMatrixRuntimeException(err);
            }

            keyStoreType = serverProps.getProperty(KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
            keyManagerFactoryAlgorithm = serverProps.getProperty(KEY_MANAGER_ALGORITHM, DEFAULT_KEY_MANAGER_ALGORITHM);
        
            authenticationMode = serverProps.getProperty(AUTHENTICATION_MODE);

            trustStoreFileName = serverProps.getProperty(TRUSTSTORE_FILENAME);
            try {
                trustStorePassword = CryptoUtil.stringDecrypt(serverProps.getProperty(TRUSTSTORE_PASSWORD, "")); //$NON-NLS-1$
            } catch (CryptoException err) {
                throw new MetaMatrixRuntimeException(err);
            }
            
            sslProtocol = serverProps.getProperty(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);
            
        } catch (ConfigurationException ce) {
            keyStoreType = DEFAULT_KEYSTORE_TYPE;
            sslProtocol = DEFAULT_SSL_PROTOCOL;
            keyManagerFactoryAlgorithm = DEFAULT_KEY_MANAGER_ALGORITHM;
            keyStoreFileName = null;
            trustStoreFileName = null;
            authenticationMode = ONEWAY;
        } finally {
            initialized = true;
        }        
    } 
    
    /**
     * Returns a client socket that will perform a custom handshake based upon the cluster key
     * 
     * @param bindAddr
     * @param port
     * @return
     * @throws IOException
     */
    public static Socket getInternalClientSocket(InetAddress bindAddr,
                                                 int port) throws IOException {
        initProperties();
        
        Socket clientSocket = new Socket(bindAddr, port);
        
        boolean success = false;
        try {
	        OutputStream out = clientSocket.getOutputStream();
	        Random r = new Random();
	        byte[] challenge = new byte[16];
	        r.nextBytes(challenge);
	        byte[] msg = CryptoUtil.getCryptor().encrypt(challenge);
	        out.write(msg);
	        InputStream in = clientSocket.getInputStream();
	        byte[] response = new byte[48];
	        in.read(response);
	        response = CryptoUtil.getCryptor().decrypt(response);
	        if (!Arrays.equals(challenge, Arrays.copyOf(response, 16))) {
	        	throw new IOException("handshake failed"); //$NON-NLS-1$
	        }
	        success = true;
        } catch (CryptoException e) {
        	throw new IOException(e);
        } finally {
        	if (!success) {
        		clientSocket.close();
        	}
        }
        
        return clientSocket;
    }

    public static SSLEngine getServerSSLEngine() throws IOException {
        initProperties();
        
        if (!isServerSSLEnabled()) {
        	return null;
        }
        
        // Use the SSLContext to create an SSLServerSocketFactory.
        SSLContext context = null;

        if (ANONYMOUS.equals(authenticationMode)) {
            context = getAnonSSLContext();
        } else {
            context = getSSLContext(keyStoreFileName,
                                    keyStorePassword,
                                    trustStoreFileName,
                                    trustStorePassword,
                                    keyManagerFactoryAlgorithm,
                                    keyStoreType,
                                    sslProtocol);
        } 

        SSLEngine result = context.createSSLEngine();
        result.setUseClientMode(false);
        if (ANONYMOUS.equals(authenticationMode)) {
            Assertion.assertTrue(Arrays.asList(result.getSupportedCipherSuites()).contains(ANON_CIPHER_SUITE));
            result.setEnabledCipherSuites(new String[] {
                ANON_CIPHER_SUITE
            });
        } 
        result.setNeedClientAuth(TWOWAY.equals(authenticationMode));
        return result;
    }

    public static ServerSocket getInternalServerSocket(int port,
                                               int backlog,
                                               InetAddress bindAddr) throws IOException {
        initProperties();
        
        ServerSocket serverSocket = new ServerSocket(port, backlog, bindAddr);
        
        return new ServerHandshakeDelegate(serverSocket);
    }
    
    private static class ServerHandshakeDelegate extends ServerSocket {
    	
    	private ServerSocket serverSocket;
    	
    	public ServerHandshakeDelegate(ServerSocket serverSocket) throws IOException {
			this.serverSocket = serverSocket;
		}

		public Socket accept() throws IOException {
			Socket socket = serverSocket.accept();
			boolean success = false;
	        try {
	        	InputStream in = socket.getInputStream();
		        byte[] response = new byte[32];
		        in.read(response);
		        response = CryptoUtil.getCryptor().decrypt(response);
		        if (response.length != 16) {
		        	throw new IOException("handshake failed"); //$NON-NLS-1$
		        }
		        
		        OutputStream out = socket.getOutputStream();
		        Random r = new Random();
		        byte[] challenge = new byte[16];
		        r.nextBytes(challenge);
		        
		        challenge = Arrays.copyOf(response, 32);
		        System.arraycopy(response, 0, challenge, 16, 16);
		        
		        byte[] msg = CryptoUtil.getCryptor().encrypt(challenge);
		        out.write(msg);
		        success = true;
	        } catch (CryptoException e) {
	        	throw new IOException(e);
	        } finally {
	        	if (!success) {
	        		socket.close();
	        	}
	        }
			return socket;
		}

		public void bind(SocketAddress arg0, int arg1) throws IOException {
			serverSocket.bind(arg0, arg1);
		}

		public void bind(SocketAddress arg0) throws IOException {
			serverSocket.bind(arg0);
		}

		public void close() throws IOException {
			serverSocket.close();
		}

		public boolean equals(Object obj) {
			return serverSocket.equals(obj);
		}

		public ServerSocketChannel getChannel() {
			return serverSocket.getChannel();
		}

		public InetAddress getInetAddress() {
			return serverSocket.getInetAddress();
		}

		public int getLocalPort() {
			return serverSocket.getLocalPort();
		}

		public SocketAddress getLocalSocketAddress() {
			return serverSocket.getLocalSocketAddress();
		}

		public int getReceiveBufferSize() throws SocketException {
			return serverSocket.getReceiveBufferSize();
		}

		public boolean getReuseAddress() throws SocketException {
			return serverSocket.getReuseAddress();
		}

		public int getSoTimeout() throws IOException {
			return serverSocket.getSoTimeout();
		}

		public int hashCode() {
			return serverSocket.hashCode();
		}

		public boolean isBound() {
			return serverSocket.isBound();
		}

		public boolean isClosed() {
			return serverSocket.isClosed();
		}

		public void setPerformancePreferences(int arg0, int arg1, int arg2) {
			serverSocket.setPerformancePreferences(arg0, arg1, arg2);
		}

		public void setReceiveBufferSize(int arg0) throws SocketException {
			serverSocket.setReceiveBufferSize(arg0);
		}

		public void setReuseAddress(boolean arg0) throws SocketException {
			serverSocket.setReuseAddress(arg0);
		}

		public void setSoTimeout(int arg0) throws SocketException {
			serverSocket.setSoTimeout(arg0);
		}

		public String toString() {
			return serverSocket.toString();
		}
    	
    }
    
    public static SSLContext getAnonSSLContext() throws IOException {
        return getSSLContext(null, null, null, null, null, null, ANON_PROTOCOL);
    }
    
    public static SSLContext getSSLContext(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol) throws IOException {
        
        try {
            // Configure the Keystore Manager
            KeyManager[] keyManagers = null;
            if (keystore != null) {
                KeyStore ks = loadKeyStore(keystore, password, keystoreType);
                if (ks != null) {
                    KeyManagerFactory kmf = KeyManagerFactory.getInstance(algorithm);
                    kmf.init(ks, password.toCharArray());
                    keyManagers = kmf.getKeyManagers();
                }
            }
            
            // Configure the Trust Store Manager
            TrustManager[] trustManagers = null;
            if (truststore != null) {
                KeyStore ks = loadKeyStore(truststore, truststorePassword, keystoreType);
                if (ks != null) {
                    TrustManagerFactory tmf = TrustManagerFactory.getInstance(algorithm);
                    tmf.init(ks);
                    trustManagers = tmf.getTrustManagers();
                }
            } 

            // Configure the SSL
            SSLContext sslc = SSLContext.getInstance(protocol);
            sslc.init(keyManagers, trustManagers, null);
            return sslc;
        } catch (GeneralSecurityException err) {
            IOException exception = new IOException(err.getMessage());
            exception.initCause(err);
            throw exception;
        } 
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
    static KeyStore loadKeyStore(String name, String password, String type) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
        
        // Check in the classpath
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        if (stream == null) {
            try {
                stream = new FileInputStream(name);
            } catch (FileNotFoundException e) {
                IOException exception = new IOException(CommonPlugin.Util.getString("SocketHelper.keystore_not_found", name)); //$NON-NLS-1$
                exception.initCause(e);
                throw exception;
            }
        }
                
        KeyStore ks = KeyStore.getInstance(type);        
        
        ks.load(stream, password != null ? password.toCharArray() : null);
        return ks;
    }
    
    public static boolean isServerSSLEnabled() {
        initProperties();
       
        return ssl_enabled && CryptoUtil.isEncryptionEnabled();
    }
    
    public static boolean isClientEncryptionEnabled() {
        initProperties();
        
        return CryptoUtil.isEncryptionEnabled() && client_encryption_enabled;
    }
    
    public static void addCipherSuite(SSLSocket socket, String cipherSuite) {
        Assertion.assertTrue(Arrays.asList(socket.getSupportedCipherSuites()).contains(cipherSuite));

        String[] suites = socket.getEnabledCipherSuites();

        String[] newSuites = new String[suites.length + 1];
        System.arraycopy(suites, 0, newSuites, 0, suites.length);
        
        newSuites[suites.length] = cipherSuite;
        
        socket.setEnabledCipherSuites(newSuites);
    }
    
    public static void addCipherSuite(SSLEngine engine, String cipherSuite) {
        Assertion.assertTrue(Arrays.asList(engine.getSupportedCipherSuites()).contains(cipherSuite));

        String[] suites = engine.getEnabledCipherSuites();

        String[] newSuites = new String[suites.length + 1];
        System.arraycopy(suites, 0, newSuites, 0, suites.length);
        
        newSuites[suites.length] = cipherSuite;
        
        engine.setEnabledCipherSuites(newSuites);
    }

    /** 
     * Allow for testing methods to control whether encryption is used 
     */
    public static void setClientEncryptionEnabled(boolean client_encryption_enabled) {
    	initProperties();
    	
        SocketHelper.client_encryption_enabled = client_encryption_enabled;
    }
    
}
