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

import java.io.IOException;
import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.ws.security.WSPasswordCallback;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.teiid.logging.LogManager;

/**
 * Signing a message is used to validate to the recipient that the message could
 * only have come from a certain sender, and that the message was not altered in
 * transit. It involves the sender encrypting a digest (hash) of the message
 * with its private key, and the recipient decrypting the hash with the sender's
 * public key, and recalculating the digest of the message to make sure the
 * message was not altered in transit (i.e., that the digest values calculated
 * by both the sender and recipient are the same). For this process to occur you
 * must ensure that the Client's public key has been imported into the server's
 * keystore using keytool. (taken CXF website)
 *
 * The properties object format must be
 * org.apache.ws.security.crypto.provider=org.apache.ws.security.components.crypto.Merlin
 * org.apache.ws.security.crypto.merlin.keystore.type=jks
 * org.apache.ws.security.crypto.merlin.keystore.password=keyStorePassword
 * org.apache.ws.security.crypto.merlin.keystore.alias=myAlias
 * org.apache.ws.security.crypto.merlin.keystore.file=client_keystore.jks
 */
public class SignatureProfile extends WSSecurityToken implements CallbackHandler {
	private String signatureUser;
	private String signaturePassword;
	private Properties signatureProperties;
	private String signatureKeyIdentifier;
	private String signatureParts;
	private String signatureAlgorithm;
	private String signatureDigestAlgorithm;

	/**
	 *
	 * @param credential
	 * @param signatureUser
	 * @param signaturePassword
	 * @param signatureProperties
	 * @param signatureKeyIdentifier - For signature <code>IssuerSerial</code> and <code>DirectReference</code> are valid only.
	 * @param signatureParts
	 * @param signatureAlgorithm
	 * @param signatureDigestAlgorithm
	 */
	public SignatureProfile(String signatureUser, String signaturePassword,
			Properties signatureProperties, String signatureKeyIdentifier,
			String signatureParts, String signatureAlgorithm,
			String signatureDigestAlgorithm) {
    	this.signatureUser = signatureUser;
    	this.signaturePassword = signaturePassword;
    	this.signatureProperties = signatureProperties;
    	this.signatureKeyIdentifier = signatureKeyIdentifier;
    	this.signatureParts = signatureParts;
    	this.signatureAlgorithm = signatureAlgorithm;
    	this.signatureDigestAlgorithm = signatureDigestAlgorithm;
        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_signature_profile")); //$NON-NLS-1$
    }

    @Override
    public void addSecurity(WSSecurityCredential credential) {
        setAction(credential, WSHandlerConstants.SIGNATURE);

        if (this.signatureUser != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.SIGNATURE_USER, this.signatureUser);
        }

        if (this.signaturePassword != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.PW_CALLBACK_REF, this);
        }

        credential.getRequestPropterties().put(WSHandlerConstants.SIG_PROP_REF_ID, this.signatureProperties);

        if (this.signatureKeyIdentifier != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.SIG_KEY_ID, this.signatureKeyIdentifier);
        }

        if (this.signatureParts != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.SIGNATURE_PARTS, this.signatureParts);
        }

        if (this.signatureAlgorithm != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.SIG_ALGO, this.signatureAlgorithm);
        }

        if (this.signatureDigestAlgorithm != null) {
        	credential.getRequestPropterties().put(WSHandlerConstants.SIG_DIGEST_ALGO, this.signatureDigestAlgorithm);
        }
    }

    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	if ((this.signatureUser != null) && (this.signaturePassword != null)) {
	        for (int i = 0; i < callbacks.length; i++) {
	            if (callbacks[i] instanceof WSPasswordCallback) {
	                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
	                pc.setPassword(this.signaturePassword);
	            }
	            else {
	                throw new UnsupportedCallbackException(callbacks[i], "unrecognized_callback"); //$NON-NLS-1$
	            }
	        }
    	}
    	else {
    		super.handleCallback(callbacks);
    	}
    }
}