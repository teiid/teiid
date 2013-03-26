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
import java.security.MessageDigest;
import java.util.Properties;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.ws.security.WSConstants;
import org.apache.ws.security.WSPasswordCallback;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.apache.ws.security.message.token.UsernameToken;
import org.apache.ws.security.util.Base64;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.TeiidSecurityCredential;


/**
 * a base class handle WSSecurity
 */
public abstract class WSSecurityToken {
    protected TeiidSecurityCredential credential;	
    private WSSecurityToken nextToken;
    private WSSecurityToken prevToken;
    
    public WSSecurityToken(TeiidSecurityCredential credential) {
        this.credential = credential;
    }
    
    Object getProperty(String name) {
        return this.credential.getRequestPropterties().get(name);
    }    
    
    public void handleSecurity() {
        addSecurity();
        if (nextToken != null) {
            this.nextToken.handleSecurity();
        }
    }

    WSSecurityToken getNextToken() {
        return nextToken;
    }   
    
    WSSecurityToken setNextToken(WSSecurityToken token) {
        this.nextToken = token;
        this.nextToken.prevToken = this;
        return token;
    }       
    
    abstract void addSecurity();
    
    void setAction(String action) {
        String prev = (String)credential.getRequestPropterties().get(WSHandlerConstants.ACTION);
        if (prev == null || prev.length() == 0) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ACTION, action);
        }
        else {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ACTION, prev+" "+action); //$NON-NLS-1$
        }
    }    
    
    void handleCallback(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	if (prevToken != null) {
    		this.prevToken.handleCallback(callbacks);
    	}
    	else {
    		throw new TeiidRuntimeException("No passwords defined for the profile"); //$NON-NLS-1$
    	}
    }
}


/**
 * Timestamp Profile using WSS4J
 */
class TimestampProfile extends WSSecurityToken {
	private boolean inMilli = true;
	private int ttl;
	
    public TimestampProfile(TeiidSecurityCredential credential, int ttl, boolean inMilli) {
        super(credential);
        this.inMilli = inMilli;
        this.ttl = ttl;
        
        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_timestamp_profile")); //$NON-NLS-1$
    }
    
    @Override
    public void addSecurity() {
        setAction(WSHandlerConstants.TIMESTAMP);
        
        // How long ( in seconds ) message is valid since send.
        this.credential.getRequestPropterties().put(WSHandlerConstants.TTL_TIMESTAMP, ttl);
        
        // if you do want to use millisecond precision set this to false; default true;
       	this.credential.getRequestPropterties().put(WSHandlerConstants.TIMESTAMP_PRECISION, Boolean.toString(inMilli));
    }
}

/**
 * This class uses the WS-Security using standard OASIS Web Services Security
 * implemented by apache "WSS4J" Implements "Username Token Profile"
 */
class UsernameTokenProfile extends WSSecurityToken implements CallbackHandler {        
    private boolean encryptedPassword;
    protected String passwd;
    protected String user;
    
    public UsernameTokenProfile(TeiidSecurityCredential credential, String user, String passwd, boolean encryptedPassword){
        super(credential);
        this.encryptedPassword = encryptedPassword;
        this.passwd = passwd;
        this.user = user;
        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_username_profile")); //$NON-NLS-1$
    }
    
    @Override
    public void addSecurity() {
        setAction(WSHandlerConstants.USERNAME_TOKEN);
        
        this.credential.getRequestPropterties().put(WSHandlerConstants.USER, user);
        if (this.encryptedPassword) {
        	this.credential.getRequestPropterties().put(UsernameToken.PASSWORD_TYPE, WSConstants.PW_DIGEST);
        }
        else {
        	this.credential.getRequestPropterties().put(UsernameToken.PASSWORD_TYPE, WSConstants.PW_TEXT);
        }        
        this.credential.getRequestPropterties().put(WSHandlerConstants.PW_CALLBACK_REF, this);  
    }
    
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof WSPasswordCallback) {
                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
                if (this.encryptedPassword) {
                    pc.setPassword(encrypt(this.passwd));
                }
                else {
                    pc.setPassword(this.passwd);
                }
            } else {
                throw new UnsupportedCallbackException(callbacks[i], "unrecognized_callback"); //$NON-NLS-1$
            }
        }            
    }
    
    String encrypt(String clearText) {
        String sha1Hash = null;
        try {
          MessageDigest md = MessageDigest.getInstance("SHA1"); //$NON-NLS-1$
          byte[] digest = md.digest(clearText.getBytes());
          sha1Hash = new String(Base64.encode(digest));
        } catch (Exception e) {
          e.printStackTrace();
        }
        return sha1Hash;       
    }
    
    @Override
    void handleCallback(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	handle(callbacks);
    }    
}
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
class EncryptProfile extends WSSecurityToken { 
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
	public EncryptProfile(TeiidSecurityCredential credential,
			String encryptionUser, Properties encryptionProperties,
			String encryptionKeyIdentifier, String encryptionParts,
			String encryptionSymAlgorithm,
			String encryptionKeyTransportAlgorithm) {
        super(credential);
        this.encryptionUser = encryptionUser;
        this.encryptionProperties = encryptionProperties;
        this.encryptionKeyIdentifier = encryptionKeyIdentifier;
        this.encryptionParts = encryptionParts;
        this.encryptionSymAlgorithm = encryptionSymAlgorithm;
        this.encryptionKeyTransportAlgorithm=encryptionKeyTransportAlgorithm;
        
        LogManager.logDetail(WSManagedConnectionFactory.UTIL.getString("using_encrypt_profile")); //$NON-NLS-1$
    }
    
    @Override
    public void addSecurity() {
        setAction(WSHandlerConstants.ENCRYPT);
        
        // if null defaults to USER property
        if (this.encryptionUser != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ENCRYPTION_USER, this.encryptionUser);
        }
        
        //Configuration of public key used to encrypt message goes to properties file.
        this.credential.getRequestPropterties().put(WSHandlerConstants.ENC_PROP_REF_ID, this.encryptionProperties);
        
        if (this.encryptionKeyIdentifier != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ENC_KEY_ID, this.encryptionKeyIdentifier);
        }
        if (this.encryptionParts != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ENCRYPTION_PARTS, this.encryptionParts);
        }
        if (this.encryptionSymAlgorithm != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ENC_SYM_ALGO, this.encryptionSymAlgorithm);
        }
        if (this.encryptionKeyTransportAlgorithm != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.ENC_KEY_TRANSPORT, this.encryptionKeyTransportAlgorithm);
        }
    }
}

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
class SignatureProfile extends WSSecurityToken implements CallbackHandler {
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
	public SignatureProfile(TeiidSecurityCredential credential,
			String signatureUser, String signaturePassword,
			Properties signatureProperties, String signatureKeyIdentifier,
			String signatureParts, String signatureAlgorithm,
			String signatureDigestAlgorithm) {
        super(credential);
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
    public void addSecurity() {
        setAction(WSHandlerConstants.SIGNATURE);
        
        if (signatureUser != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.SIGNATURE_USER, this.signatureUser);
        }
        
        if (this.signaturePassword != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.PW_CALLBACK_REF, this);
        }
        
        this.credential.getRequestPropterties().put(WSHandlerConstants.SIG_PROP_REF_ID, this.signatureProperties);
        
        if (this.signatureKeyIdentifier != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.SIG_KEY_ID, this.signatureKeyIdentifier);
        }
        
        if (this.signatureParts != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.SIGNATURE_PARTS, this.signatureParts);
        }
        
        if (this.signatureAlgorithm != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.SIG_ALGO, this.signatureAlgorithm);
        }
        
        if (this.signatureDigestAlgorithm != null) {
        	this.credential.getRequestPropterties().put(WSHandlerConstants.SIG_DIGEST_ALGO, this.signatureDigestAlgorithm);
        }
    }   
    
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
    	if (this.signatureUser != null && this.signaturePassword != null) {
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


/**
 * SAML Profile based authentication using WSS4J. 
 * 
 * @param signed - if true must accompny with {@link SignatureProfile}
 * 
 * format for saml.properties file 
 * org.apache.ws.security.saml.issuerClass=org.apache.ws.security.saml.SAMLIssuerImpl
 * org.apache.ws.security.saml.issuer=www.example.com
 * org.apache.ws.security.saml.issuer.cryptoProp.file=outsecurity.properties
 * org.apache.ws.security.saml.issuer.key.name=myalias
 * org.apache.ws.security.saml.issuer.key.password=myAliasPassword
 */
class SAMLTokenProfile extends WSSecurityToken {
    private boolean signed = false;
    private String samlPropFile;
    private CallbackHandler handler;
    
    public SAMLTokenProfile(TeiidSecurityCredential credential, boolean signed, String samlPropFile, CallbackHandler handler){
        super(credential);
        this.signed = signed;
        this.samlPropFile = samlPropFile;
        this.handler = handler;
    }

    @Override
    public void addSecurity() {
        if (this.signed) {
            setAction(WSHandlerConstants.SAML_TOKEN_SIGNED);
        }
        else {
            setAction(WSHandlerConstants.SAML_TOKEN_UNSIGNED);   
        }
        this.credential.getRequestPropterties().put(WSHandlerConstants.SAML_PROP_FILE, this.samlPropFile); 
        this.credential.getRequestPropterties().put(WSHandlerConstants.SAML_CALLBACK_REF, this.handler);
    }            
}