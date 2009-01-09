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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.metamatrix.common.comm.platform.socket.SocketUtil;
import com.metamatrix.common.config.CurrentConfiguration;
import com.metamatrix.common.config.ResourceNames;
import com.metamatrix.common.config.api.exceptions.ConfigurationException;
import com.metamatrix.common.util.CommonPropertyNames;
import com.metamatrix.common.util.PropertiesUtils;
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
public class ServerSocketConfiguration {

    private static final String SSL_ENABLED       = "metamatrix.encryption.secure.sockets"; //$NON-NLS-1$
    
    private static final String KEYSTORE_FILENAME = "com.metamatrix.ssl.keystore.filename"; //$NON-NLS-1$
    private static final String KEYSTORE_PASSWORD = "com.metamatrix.ssl.keystore.Password"; //$NON-NLS-1$
    private static final String KEYSTORE_TYPE     = "com.metamatrix.ssl.keystoretype"; //$NON-NLS-1$
    private static final String SSL_PROTOCOL      = "com.metamatrix.ssl.protocol"; //$NON-NLS-1$
    private static final String KEY_MANAGER_ALGORITHM = "com.metamatrix.ssl.keymanagementalgorithm"; //$NON-NLS-1$
    
    private static final String TRUSTSTORE_FILENAME = "com.metamatrix.ssl.truststore.filename"; //$NON-NLS-1$
    private static final String TRUSTSTORE_PASSWORD = "com.metamatrix.ssl.truststore.Password"; //$NON-NLS-1$
    private static final String AUTHENTICATION_MODE         = "com.metamatrix.ssl.authenticationMode"; //$NON-NLS-1$
    public static final String ONEWAY = "1-way"; //$NON-NLS-1$ - one way is the default
    public static final String TWOWAY = "2-way"; //$NON-NLS-1$
    public static final String ANONYMOUS = "anonymous"; //$NON-NLS-1$

    public static final String DEFAULT_SSL_PROTOCOL = "SSLv3"; //$NON-NLS-1$
    public static final String DEFAULT_KEY_MANAGER_ALGORITHM = "SunX509"; //$NON-NLS-1$
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    public static final String UNENCRYPTED_CIPHER_SUITE = "SSL_RSA_WITH_NULL_SHA"; //$NON-NLS-1$
    
    /*
     * External SSL resource settings
     */
    private boolean ssl_enabled;
    private String sslProtocol = DEFAULT_SSL_PROTOCOL;
    private String keyManagerFactoryAlgorithm = DEFAULT_KEY_MANAGER_ALGORITHM;
    private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
    private String keyStoreFileName;
    private String keyStorePassword = ""; //$NON-NLS-1$
    private String trustStoreFileName;
    private String trustStorePassword = ""; //$NON-NLS-1$
    private String authenticationMode = ONEWAY;
    
    /*
     * Client encryption property.  This may belong somewhere else
     */
    boolean client_encryption_enabled = false;
    
    public static boolean isSSLEnabled() throws ConfigurationException {
    	return CryptoUtil.isEncryptionEnabled() && PropertiesUtils.getBooleanProperty(CurrentConfiguration.getProperties(), SSL_ENABLED, false);
    }
    
    public void init() throws ConfigurationException {
    	Properties p = CurrentConfiguration.getProperties();
    	p = PropertiesUtils.clone(CurrentConfiguration.getResourceProperties(ResourceNames.SSL), p, true);
    	init(p);
    }
    
    public void init(Properties props) {
        ssl_enabled = PropertiesUtils.getBooleanProperty(props, SSL_ENABLED, false); 
        
        client_encryption_enabled = PropertiesUtils.getBooleanProperty(props, CommonPropertyNames.CLIENT_ENCRYPTION_ENABLED, true);
        
        keyStoreFileName = props.getProperty(KEYSTORE_FILENAME);
        try {
            keyStorePassword = CryptoUtil.stringDecrypt(props.getProperty(KEYSTORE_PASSWORD, "")); //$NON-NLS-1$
        } catch (CryptoException err) {
            throw new MetaMatrixRuntimeException(err);
        }

        keyStoreType = props.getProperty(KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE);
        keyManagerFactoryAlgorithm = props.getProperty(KEY_MANAGER_ALGORITHM, DEFAULT_KEY_MANAGER_ALGORITHM);
    
        authenticationMode = props.getProperty(AUTHENTICATION_MODE);

        trustStoreFileName = props.getProperty(TRUSTSTORE_FILENAME);
        try {
            trustStorePassword = CryptoUtil.stringDecrypt(props.getProperty(TRUSTSTORE_PASSWORD, "")); //$NON-NLS-1$
        } catch (CryptoException err) {
            throw new MetaMatrixRuntimeException(err);
        }
        
        sslProtocol = props.getProperty(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);            
    } 
    
    /**
     * Returns a client socket that will perform a custom handshake based upon the cluster key
     * 
     * @param bindAddr
     * @param port
     * @return
     * @throws IOException
     */
    public Socket getInternalClientSocket(InetAddress bindAddr,
                                                 int port) throws IOException {
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

    public SSLEngine getServerSSLEngine() throws IOException {
        if (!isServerSSLEnabled()) {
        	return null;
        }
        
        // Use the SSLContext to create an SSLServerSocketFactory.
        SSLContext context = null;

        if (ANONYMOUS.equals(authenticationMode)) {
            context = SocketUtil.getAnonSSLContext();
        } else {
            context = SocketUtil.getSSLContext(keyStoreFileName,
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
            Assertion.assertTrue(Arrays.asList(result.getSupportedCipherSuites()).contains(SocketUtil.ANON_CIPHER_SUITE));
            result.setEnabledCipherSuites(new String[] {
            		SocketUtil.ANON_CIPHER_SUITE
            });
        } 
        result.setNeedClientAuth(TWOWAY.equals(authenticationMode));
        return result;
    }

    public ServerSocket getInternalServerSocket(int port,
                                               int backlog,
                                               InetAddress bindAddr) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port, backlog, bindAddr);
        
        return new ServerHandshakeDelegate(serverSocket);
    }
    
    private class ServerHandshakeDelegate extends ServerSocket {
    	
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
    
    public boolean isServerSSLEnabled() {
        return ssl_enabled && CryptoUtil.isEncryptionEnabled();
    }
    
    public boolean isClientEncryptionEnabled() {
        return CryptoUtil.isEncryptionEnabled() && client_encryption_enabled;
    }
    
}
