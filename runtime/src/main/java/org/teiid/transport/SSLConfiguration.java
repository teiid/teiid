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

package org.teiid.transport;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringTokenizer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.teiid.net.socket.SocketUtil;
import org.teiid.runtime.RuntimePlugin;



public class SSLConfiguration {

    public static final String ONEWAY = "1-way"; //$NON-NLS-1$ - one way is the default
    public static final String TWOWAY = "2-way"; //$NON-NLS-1$
    public static final String ANONYMOUS = "anonymous"; //$NON-NLS-1$
    
    public static final String LOGIN = "logIn"; //$NON-NLS-1$
    public static final String DISABLED = "disabled"; //$NON-NLS-1$
    public static final String ENABLED = "enabled"; //$NON-NLS-1$

    private static final String DEFAULT_KEYSTORE_TYPE = "JKS"; //$NON-NLS-1$
    
    /*
     * External SSL resource settings
     */
    private String mode = LOGIN;
    private String sslProtocol = SocketUtil.DEFAULT_PROTOCOL;
    private String keyManagerFactoryAlgorithm = KeyManagerFactory.getDefaultAlgorithm();
    private String keyStoreType = DEFAULT_KEYSTORE_TYPE;
    private String keyStoreFileName;
    private String keyStorePassword = ""; //$NON-NLS-1$
    private String trustStoreFileName;
    private String trustStorePassword = ""; //$NON-NLS-1$
    private String authenticationMode = ONEWAY;
    private String[] enabledCipherSuites;
    

	public SSLEngine getServerSSLEngine() throws IOException, GeneralSecurityException {
        if (!isSslEnabled()) {
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
            if (!(Arrays.asList(result.getSupportedCipherSuites()).contains(SocketUtil.ANON_CIPHER_SUITE))) {
            	throw new GeneralSecurityException(RuntimePlugin.Util.getString("SSLConfiguration.no_anonymous")); //$NON-NLS-1$
            }
            result.setEnabledCipherSuites(this.enabledCipherSuites == null?new String[] {SocketUtil.ANON_CIPHER_SUITE}:this.enabledCipherSuites);
        } else {
        	if (this.enabledCipherSuites != null) {
        		result.setEnabledCipherSuites(this.enabledCipherSuites);
        	}
        }
        
        result.setNeedClientAuth(TWOWAY.equals(authenticationMode));
        return result;
    }

    public boolean isClientEncryptionEnabled() {
        return LOGIN.equals(mode);
    }
    
    public boolean isSslEnabled() {
    	return ENABLED.equals(mode);
    }
    
    public String getMode() {
		return mode;
	}
    
    public void setMode(String mode) {
		this.mode = mode;
	}
    
    public void setKeystoreFilename(String value) {
    	this.keyStoreFileName = value;
    }
    
    public void setKeystorePassword(String value) {
    	this.keyStorePassword = value;
    }
    
    public void setKeystoreType(String value) {
    	this.keyStoreType = value;
    }
    
    public void setSslProtocol(String value) {
    	this.sslProtocol = value;
    }
    
    public void setKeymanagementAlgorithm(String value) {
    	this.keyManagerFactoryAlgorithm = value;
    }
    
    public void setTruststoreFilename(String value) {
    	this.trustStoreFileName = value;
    }
    
    public void setTruststorePassword(String value) {
    	this.trustStorePassword = value;
    }
    
    public void setAuthenticationMode(String value) {
    	this.authenticationMode = value;
    }
    
	public void setEnabledCipherSuites(String enabledCipherSuites) {
		ArrayList<String> ciphers = new ArrayList<String>();
		StringTokenizer st = new StringTokenizer(enabledCipherSuites);
		while(st.hasMoreTokens()) {
			ciphers.add(st.nextToken().trim());
		}
		
		if (!ciphers.isEmpty()) {
			this.enabledCipherSuites = ciphers.toArray(new String[ciphers.size()]);
		}
	}    
}
