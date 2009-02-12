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

package com.metamatrix.common.comm.platform.socket;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.Arrays;
import java.util.Properties;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import com.metamatrix.common.comm.platform.CommPlatformPlugin;
import com.metamatrix.core.util.Assertion;


/** 
 * This class provides some utility methods to create ssl sockets using the
 * keystores and trust stores. these are the properties required for the making the 
 * ssl connection
 * <p>
 * The search for the key stores is follows the path
 * MM defined properties ---> javax defined properties
 * <p/>
 * <ul>
 * <b>2-way SSL (MetaMatrix based)</b>
 * <li>-Dcom.metamatrix.ssl.keyStore (required)
 * <li>-Dcom.metamatrix.ssl.keyStorePassword (required)
 * <li>-Dcom.metamatrix.ssl.trustStore (required)
 * <li>-Dcom.metamatrix.ssl.trustStorePassword (required)
 * <li>-Dcom.metamatrix.ssl.protocol (optional;default=SSLv3)
 * <li>-Dcom.metamatrix.ssl.algorithm (optional;default=SunX509)
 * <li>-Dcom.metamatrix.ssl.keyStoreType (optional;default=JKS)
 * </ul>
 * <p/>
 * <ul>
 * <b>2-way SSL (javax based; can used where there are no conflicts in JVM)</b>
 * <li>-Djavax.net.ssl.keyStore (required)
 * <li>-Djavax.net.ssl.keyStorePassword (required)
 * <li>-Djavax.net.ssl.trustStore (required)
 * <li>-Djavax.net.ssl.trustStorePassword (required)
 * <li>-Djavax.net.ssl.keyStoreType (optional)
 * </ul>
 * <p/>
 * <ul>
 * <b>1-way SSL (metamatrix Based)</b>
 * <li>-Dcom.metamatrix.ssl.trustStore (required)
 * <li>-Dcom.metamatrix.ssl.trustStorePassword (required)
 * <li>-Dcom.metamatrix.ssl.protocol (optional;default=SSLv3)
 * <li>-Dcom.metamatrix.ssl.algorithm (optional;default=SunX509)
 * <li>-Dcom.metamatrix.ssl.keyStoreType (optional;default=JKS)
 * </ul>
 * <p/>
 * <ul>
 * <b>1-way SSL (javax based; can used where there are no conflicts in JVM)</b>
 * <li>-Djavax.net.ssl.trustStore (required)
 * <li>-Djavax.net.ssl.trustStorePassword (required)
 * <li>-Djavax.net.ssl.keyStoreType (optional)
 * </ul>
 * 
 */
public class SocketUtil {
    
    static final String TRUSTSTORE_PASSWORD = "com.metamatrix.ssl.trustStorePassword"; //$NON-NLS-1$
    public static final String TRUSTSTORE_FILENAME = "com.metamatrix.ssl.trustStore"; //$NON-NLS-1$
    static final String KEYSTORE_ALGORITHM = "com.metamatrix.ssl.algorithm"; //$NON-NLS-1$
    static final String PROTOCOL = "com.metamatrix.ssl.protocol"; //$NON-NLS-1$
    static final String KEYSTORE_TYPE = "com.metamatrix.ssl.keyStoreType"; //$NON-NLS-1$
    static final String KEYSTORE_PASSWORD = "com.metamatrix.ssl.keyStorePassword"; //$NON-NLS-1$
    static final String KEYSTORE_FILENAME = "com.metamatrix.ssl.keyStore"; //$NON-NLS-1$
    
    static final String DEFAULT_ALGORITHM = "SunX509"; //$NON-NLS-1$
    static final String DEFAULT_KEYSTORE_PROTOCOL = "SSLv3"; //$NON-NLS-1$
    static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    public static final String NONE = "none"; //$NON-NLS-1$
    
    public static final String ANON_CIPHER_SUITE = "TLS_DH_anon_WITH_AES_128_CBC_SHA"; //$NON-NLS-1$
    public static final String ANON_PROTOCOL = "TLS"; //$NON-NLS-1$
    
    public static class SSLEngineFactory {
    	private boolean isAnon;
    	private SSLContext context;
    	
    	public SSLEngineFactory(SSLContext context, boolean isAnon) {
			this.context = context;
			this.isAnon = isAnon;
		}

		public SSLEngine getSSLEngine() {
    		SSLEngine result = context.createSSLEngine();
    		result.setUseClientMode(true);
    		if (isAnon) {
    			addCipherSuite(result, ANON_CIPHER_SUITE);
    		}
    		return result;
    	}
    }
    
    public static SSLEngineFactory getSSLEngineFactory(Properties props) throws IOException, NoSuchAlgorithmException{
    	// -Dcom.metamatrix.ssl.keyStore
        String keystore = props.getProperty(KEYSTORE_FILENAME); 
        // -Dcom.metamatrix.ssl.keyStorePassword
        String keystorePassword = props.getProperty(KEYSTORE_PASSWORD); 
        // -Dcom.metamatrix.ssl.keyStoreType (default JKS)
        String keystoreType = props.getProperty(KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE); 
        // -Dcom.metamatrix.ssl.protocol (default SSLv3)
        String keystoreProtocol = props.getProperty(PROTOCOL, DEFAULT_KEYSTORE_PROTOCOL); 
        // -Dcom.metamatrix.ssl.algorithm (default SunX509)
        String keystoreAlgorithm = props.getProperty(KEYSTORE_ALGORITHM, DEFAULT_ALGORITHM); 
        // -Dcom.metamatrix.ssl.trustStore (if null; keystore filename used)
        String truststore = props.getProperty(TRUSTSTORE_FILENAME, keystore); 
        // -Dcom.metamatrix.ssl.trustStorePassword (if null; keystore password used)
        String truststorePassword = props.getProperty(TRUSTSTORE_PASSWORD, keystorePassword); 
        
        boolean anon = NONE.equalsIgnoreCase(truststore);
        
        SSLContext result = null;
        // 0) anon
        // 1) keystore != null = 2 way SSL (can define a separate truststore too)
        // 2) truststore != null = 1 way SSL (here we can define custom properties for truststore; useful when 
        //    client like a appserver have to define multiple certs without importing 
        //    all the certificates into one single certificate
        // 3) else = javax properties; this is default way to define the SSL anywhere.
        if (anon) {
        	result = getAnonSSLContext();
        } else if (keystore != null) {
            // 2 way SSL
            result = getClientSSLContext(keystore, keystorePassword, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        } else  if(truststore != null) {
            // One way SSL with custom properties defined
            result = getClientSSLContext(null, null, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        } else {
        	result = SSLContext.getDefault();
        }
        return new SSLEngineFactory(result, anon);
    }
    
    /**
     * create socket factory for the client socket.  
     */
    static SSLContext getClientSSLContext(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol) throws IOException {
        return getSSLContext(keystore, password, truststore, truststorePassword, algorithm, keystoreType, protocol);
    }
    
    public static void addCipherSuite(SSLEngine engine, String cipherSuite) {
        Assertion.assertTrue(Arrays.asList(engine.getSupportedCipherSuites()).contains(cipherSuite));

        String[] suites = engine.getEnabledCipherSuites();

        String[] newSuites = new String[suites.length + 1];
        System.arraycopy(suites, 0, newSuites, 0, suites.length);
        
        newSuites[suites.length] = cipherSuite;
        
        engine.setEnabledCipherSuites(newSuites);
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
    public static KeyStore loadKeyStore(String name, String password, String type) throws IOException, NoSuchAlgorithmException, CertificateException, KeyStoreException {
        
        // Check in the classpath
        InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(name);
        if (stream == null) {
            try {
                stream = new FileInputStream(name);
            } catch (FileNotFoundException e) {
                IOException exception = new IOException(CommPlatformPlugin.Util.getString("SocketHelper.keystore_not_found", name)); //$NON-NLS-1$
                exception.initCause(e);
                throw exception;
            }
        }
                
        KeyStore ks = KeyStore.getInstance(type);        
        
        ks.load(stream, password != null ? password.toCharArray() : null);
        return ks;
    }

}
