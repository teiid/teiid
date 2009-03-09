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

package com.metamatrix.common.net;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import javax.net.ssl.KeyManagerFactory;
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
    public static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    public static final String UNENCRYPTED_CIPHER_SUITE = "SSL_RSA_WITH_NULL_SHA"; //$NON-NLS-1$
    
    /*
     * External SSL resource settings
     */
    private boolean ssl_enabled;
    private String sslProtocol = DEFAULT_SSL_PROTOCOL;
    private String keyManagerFactoryAlgorithm;
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
    
    public static boolean isSSLEnabled() {
    	return CryptoUtil.isEncryptionEnabled() && PropertiesUtils.getBooleanProperty(CurrentConfiguration.getInstance().getProperties(), SSL_ENABLED, false);
    }
    
    public void init() throws ConfigurationException {
    	Properties p = CurrentConfiguration.getInstance().getProperties();
    	p = PropertiesUtils.clone(CurrentConfiguration.getInstance().getResourceProperties(ResourceNames.SSL), p, true);
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
                 
        keyManagerFactoryAlgorithm = props.getProperty(KEY_MANAGER_ALGORITHM, KeyManagerFactory.getDefaultAlgorithm());
    
        authenticationMode = props.getProperty(AUTHENTICATION_MODE);

        trustStoreFileName = props.getProperty(TRUSTSTORE_FILENAME);
        try {
            trustStorePassword = CryptoUtil.stringDecrypt(props.getProperty(TRUSTSTORE_PASSWORD, "")); //$NON-NLS-1$
        } catch (CryptoException err) {
            throw new MetaMatrixRuntimeException(err);
        }
        
        sslProtocol = props.getProperty(SSL_PROTOCOL, DEFAULT_SSL_PROTOCOL);            
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

    public boolean isServerSSLEnabled() {
        return ssl_enabled && CryptoUtil.isEncryptionEnabled();
    }
    
    public boolean isClientEncryptionEnabled() {
        return CryptoUtil.isEncryptionEnabled() && client_encryption_enabled;
    }
    
}
