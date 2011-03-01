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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

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
    
    static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    public static final String ANON_CIPHER_SUITE = "TLS_DH_anon_WITH_AES_128_CBC_SHA"; //$NON-NLS-1$
    public static final String DEFAULT_PROTOCOL = "TLSv1"; //$NON-NLS-1$
    
    public static class SSLSocketFactory {
    	private boolean isAnon;
    	private boolean warned;
    	private javax.net.ssl.SSLSocketFactory factory;
    	
    	public SSLSocketFactory(SSLContext context, boolean isAnon) {
			this.factory = context.getSocketFactory();
			this.isAnon = isAnon;
		}

		public synchronized Socket getSocket() throws IOException {
    		SSLSocket result = (SSLSocket)factory.createSocket();
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
        
        boolean anon = PropertiesUtils.getBooleanProperty(props, ALLOW_ANON, true);
        
        SSLContext result = null;
        // 1) keystore != null = 2 way SSL (can define a separate truststore too)
        // 2) truststore != null = 1 way SSL (here we can define custom properties for truststore; useful when 
        //    client like a appserver have to define multiple certs without importing 
        //    all the certificates into one single certificate
        // 3) else = javax properties; this is default way to define the SSL anywhere.
        if (keystore != null) {
            // 2 way SSL
            result = getClientSSLContext(keystore, keystorePassword, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        } else  if(truststore != null) {
            // One way SSL with custom properties defined
            result = getClientSSLContext(null, null, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        } else {
        	result = SSLContext.getDefault();
        }
        return new SSLSocketFactory(result, anon);
    }
    
    /**
     * create socket factory for the client socket.  
     * @throws GeneralSecurityException 
     */
    static SSLContext getClientSSLContext(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol) throws IOException, GeneralSecurityException {
        return getSSLContext(keystore, password, truststore, truststorePassword, algorithm, keystoreType, protocol);
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
        return getSSLContext(null, null, null, null, null, null, DEFAULT_PROTOCOL);
    }
    
    public static SSLContext getSSLContext(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol) throws IOException, GeneralSecurityException {
        
    	if (algorithm == null) {
    		algorithm = KeyManagerFactory.getDefaultAlgorithm();
    	}
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

}
