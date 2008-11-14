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

package com.metamatrix.common.comm.platform.socket;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import com.metamatrix.common.net.SocketHelper;


/** 
 * This class provides some utility methods to create ssl sockets using the
 * keystores and trust stores. these are the properties required for the making the 
 * ssl connection
 * <p>
 * The search for the key stores is follows the path
 * MM defined properties ---> javax defined properties ---> default mm.keystore --> not use SSL
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
 * or it will look for the "metamatrix.keystore & metamatrix.truststore" in the classpath. The default password for these 
 * are "changeit", if user changes this password, they need to use one of the above password keys to supply the new one.
 * the same rules apply as to which passcode it looks at first.
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
    
    static final String JAVAX_TRUSTSTORE_FILENAME = "javax.net.ssl.trustStore"; //$NON-NLS-1$
    static final String JAVAX_KEYSTORE_FILENAME = "javax.net.ssl.keyStore"; //$NON-NLS-1$
    static final String JAVAX_TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword"; //$NON-NLS-1$
    static final String JAVAX_KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword"; //$NON-NLS-1$    
    
    static final String DEFAULT_ALGORITHM = "SunX509"; //$NON-NLS-1$
    static final String DEFAULT_KEYSTORE_PROTOCOL = "SSLv3"; //$NON-NLS-1$
    static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    static final String DEFAULT_KEYSTORE = "metamatrix.keystore"; //$NON-NLS-1$
    static final String DEFAULT_TRUSTSTORE = "metamatrix.truststore"; //$NON-NLS-1$
    static final String DEFAULT_PASSWORD = "changeit"; //$NON-NLS-1$
    
    public static final String NONE = "none"; //$NON-NLS-1$
    
    public static SSLEngine getClientSSLEngine() throws IOException, NoSuchAlgorithmException{
    	// -Dcom.metamatrix.ssl.keyStore
        String keystore = System.getProperty(KEYSTORE_FILENAME); 
        // -Dcom.metamatrix.ssl.keyStorePassword
        String keystorePassword = System.getProperty(KEYSTORE_PASSWORD); 
        // -Dcom.metamatrix.ssl.keyStoreType (default JKS)
        String keystoreType = System.getProperty(KEYSTORE_TYPE, DEFAULT_KEYSTORE_TYPE); 
        // -Dcom.metamatrix.ssl.protocol (default SSLv3)
        String keystoreProtocol = System.getProperty(PROTOCOL, DEFAULT_KEYSTORE_PROTOCOL); 
        // -Dcom.metamatrix.ssl.algorithm (default SunX509)
        String keystoreAlgorithm = System.getProperty(KEYSTORE_ALGORITHM, DEFAULT_ALGORITHM); 
        // -Dcom.metamatrix.ssl.trustStore (if null; keystore filename used)
        String truststore = System.getProperty(TRUSTSTORE_FILENAME, keystore); 
        // -Dcom.metamatrix.ssl.trustStorePassword (if null; keystore password used)
        String truststorePassword = System.getProperty(TRUSTSTORE_PASSWORD, keystorePassword); 
        
        boolean anon = NONE.equalsIgnoreCase(truststore);
        
        if (anon) {
        	SSLContext context = SocketHelper.getAnonSSLContext();
        	SSLEngine result = context.createSSLEngine();
        	result.setUseClientMode(true);
            SocketHelper.addCipherSuite(result, SocketHelper.ANON_CIPHER_SUITE);
            return result;
        }
                
        // 1) keystore != null = 2 way SSL (can define a separate truststore too)
        // 2) truststore != null = 1 way SSL (here we can define custom properties for truststore; useful when 
        //    client like a appserver have to define multiple certs without importing 
        //    all the certificates into one single certificate
        // 3) else = javax properties; this is default way to define the SSL anywhere.
        if (keystore != null) {
            // 2 way SSL
            return getClientSSLEngine(keystore, keystorePassword, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        }
        if(truststore != null) {
            // One way SSL with custom properties defined
            return getClientSSLEngine(null, null, truststore, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        }
        // default ssl connection if using one-way/two-way SSL depending upon the 
        // system properties set.
        // -Djavax.net.ssl.trustStore=mmdemotruststore
        // -Djavax.net.ssl.trustStorePassword=metamatrix                                    
        keystore = System.getProperty(JAVAX_KEYSTORE_FILENAME); 
        truststore = System.getProperty(JAVAX_TRUSTSTORE_FILENAME); 
        if (keystore != null || truststore != null) {
            SSLEngine result = SSLContext.getDefault().createSSLEngine();
            result.setUseClientMode(true);
            return result;
        }
        // the default scheme only works by the classloader loading mechanism.
        InputStream ksStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_KEYSTORE);
        InputStream tsStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(DEFAULT_TRUSTSTORE);

        // Now see if the customer changed the passwords in the system?
        if (keystorePassword == null) {
            keystorePassword = System.getProperty(JAVAX_KEYSTORE_PASSWORD, DEFAULT_PASSWORD);
        }

        if (truststorePassword == null) {
            truststorePassword = System.getProperty(JAVAX_TRUSTSTORE_PASSWORD, DEFAULT_PASSWORD);
        }
        
        if (ksStream != null && tsStream != null) {
            return getClientSSLEngine(DEFAULT_KEYSTORE, keystorePassword, DEFAULT_TRUSTSTORE, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        }
        if (ksStream == null && tsStream != null) {
            return getClientSSLEngine(DEFAULT_TRUSTSTORE, truststorePassword, DEFAULT_TRUSTSTORE, truststorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        }
        if (ksStream != null && tsStream == null) {
            return getClientSSLEngine(DEFAULT_KEYSTORE, keystorePassword, DEFAULT_KEYSTORE, keystorePassword, keystoreAlgorithm, keystoreType, keystoreProtocol);
        }
        SSLEngine result = SSLContext.getDefault().createSSLEngine();
        result.setUseClientMode(true);
        return result;
    }
    
    /**
     * create socket factory for the client socket.  
     */
    static SSLEngine getClientSSLEngine(String keystore,
                                            String password,
                                            String truststore,
                                            String truststorePassword,
                                            String algorithm,
                                            String keystoreType,
                                            String protocol) throws IOException {
        SSLContext context = SocketHelper.getSSLContext(keystore, password, truststore, truststorePassword, algorithm, keystoreType, protocol);
        SSLEngine result = context.createSSLEngine();
        result.setUseClientMode(true);
        return result;
    }

}
