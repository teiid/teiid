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
package org.teiid.resource.adapter.ws;

import java.util.Properties;

import org.apache.ws.security.handler.WSHandlerConstants;
import org.teiid.logging.LogManager;

/**
 * Encrypt the message
 *
 * ex: https://access.redhat.com/knowledge/docs/en-US/JBoss_Enterprise_Web_Platform/5/html/JBoss_WS_CXF_User_Guide/ch11s02.html
 *
 * The properties object format must be
 * org.apache.ws.security.crypto.provider=org.apache.ws.security.components.crypto.Merlin
 * org.apache.ws.security.crypto.merlin.keystore.type=jks
 * org.apache.ws.security.crypto.merlin.keystore.password=password
 * org.apache.ws.security.crypto.merlin.keystore.alias=alice
 * org.apache.ws.security.crypto.merlin.keystore.file=org/apache/cxf/systest/ws/security/alice.jks
 */
public class EncryptProfile extends WSSecurityToken {
	private String encryptionUser;
	private Properties encryptionProperties;
	private String encryptionKeyIdentifier;
	private String encryptionParts;
	private String encryptionSymAlgorithm;
	private String encryptionKeyTransportAlgorithm;

	/**
	 *
	 * @param credential
	 * @param encryptionUser - User name, if null takes "user" property
	 * @param encryptionProperties
	 * @param encryptionKeyIdentifier - For encryption <code>IssuerSerial</code>,
     * <code>X509KeyIdentifier</code>,  <code>DirectReference</code>,
     * <code>Thumbprint</code>, <code>SKIKeyIdentifier</code>, and
     * <code>EmbeddedKeyName</code> are valid only.
	 * @param encryptionParts - Leave as null for defaults to Body;
	 * @param encryptionSymAlgorithm
	 * @param encryptionKeyTransportAlgorithm
	 */
	public EncryptProfile(WSSecurityCredential credential,
			String encryptionUser, Properties encryptionProperties,
			String encryptionKeyIdentifier, String encryptionParts,
			String encryptionSymAlgorithm,
			String encryptionKeyTransportAlgorithm) {
        this.encryptionUser = encryptionUser;
        this.encryptionProperties = encryptionProperties;
        this.encryptionKeyIdentifier = encryptionKeyIdentifier;
        this.encryptionParts = encryptionParts;
        this.encryptionSymAlgorithm = encryptionSymAlgorithm;
        this.encryptionKeyTransportAlgorithm=encryptionKeyTransportAlgorithm;

        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_encrypt_profile")); //$NON-NLS-1$
    }

    @Override
    public void addSecurity(WSSecurityCredential credential) {
        setAction(credential, WSHandlerConstants.ENCRYPT);

        // if null defaults to USER property
        if (this.encryptionUser != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ENCRYPTION_USER, this.encryptionUser);
        }

        //Configuration of public key used to encrypt message goes to properties file.
        credential.getRequestPropterties().put(WSHandlerConstants.ENC_PROP_REF_ID, this.encryptionProperties);

        if (this.encryptionKeyIdentifier != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ENC_KEY_ID, this.encryptionKeyIdentifier);
        }
        if (this.encryptionParts != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ENCRYPTION_PARTS, this.encryptionParts);
        }
        if (this.encryptionSymAlgorithm != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ENC_SYM_ALGO, this.encryptionSymAlgorithm);
        }
        if (this.encryptionKeyTransportAlgorithm != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.ENC_KEY_TRANSPORT, this.encryptionKeyTransportAlgorithm);
        }
    }
}