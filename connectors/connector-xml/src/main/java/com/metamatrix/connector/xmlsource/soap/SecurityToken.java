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

package com.metamatrix.connector.xmlsource.soap;

import java.io.IOException;
import java.security.MessageDigest;
import java.util.StringTokenizer;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.axis.client.Call;
import org.apache.ws.security.WSConstants;
import org.apache.ws.security.WSPasswordCallback;
import org.apache.ws.security.handler.WSHandlerConstants;
import org.apache.ws.security.message.token.UsernameToken;
import org.apache.ws.security.util.Base64;
import org.teiid.connector.api.ConnectorEnvironment;

import com.metamatrix.connector.xml.TrustedPayloadHandler;
import com.metamatrix.connector.xmlsource.XMLSourcePlugin;

/**
 * Security provider for calling the Web Service 
 */
public abstract class SecurityToken {
    public static String HTTP_BASIC_AUTH = "HTTPBasic"; //$NON-NLS-1$
    public static String USERNAME_TOKEN_PROFILE_CLEAR_TEXT = "UsernameToken"; //$NON-NLS-1$
    public static String USERNAME_TOKEN_PROFILE_DIGEST = "UsernameToken-Digest"; //$NON-NLS-1$
    public static String SAML_TOKEN_UNSIGNED = "SAMLTokenUnsigned"; //$NON-NLS-1$
    public static String SAML_TOKEN_SIGNED = "SAMLTokenSigned"; //$NON-NLS-1$
    public static String SIGNATURE = "Signature"; //$NON-NLS-1$
    public static String TIMESTAMP = "Timestamp"; //$NON-NLS-1$
    public static String ENCRYPT = "Encrypt"; //$NON-NLS-1$
    public static String NONE_PROFILE= "None"; //$NON-NLS-1$
    public static String WS_SECURITY= "WS-Security"; //$NON-NLS-1$
    
    // trust certificate types.
    // The issuer-serial method presumes that all trusted users of the service 
    // are known to the service and have pre-registered their certificate chains 
    // before using the service. The direct-reference method presumes that the 
    // service operator trusts all users with certificates issued by a trusted CA. 
    // http://ws.apache.org/wss4j/cert.html
    public static final String ISSUER_SERIAL = "IssuerSerial"; //$NON-NLS-1$
    public static final String DIRECT_REFERENCE = "DirectReference"; //$NON-NLS-1$
    
    ConnectorEnvironment env;
    String username;
    String password;
    
    public static SecurityToken getSecurityToken(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler) {
        // first find out what type of security we are going to handle
        String securityType = env.getProperties().getProperty(SoapConnectorProperties.AUTHORIZATION_TYPE);
        
        if (securityType != null) {            
            // the first two are non-ws-security based; and they can not be nested.
            if (securityType.equalsIgnoreCase(NONE_PROFILE)) {
                return new NoProvider(env, trustedPayloadHandler);
            }                
            else if (securityType.equalsIgnoreCase(HTTP_BASIC_AUTH)) {
                return new HTTPBasic(env, trustedPayloadHandler);
            }
            else if (securityType.equalsIgnoreCase(WS_SECURITY)) {   

                String wsSecurityType = env.getProperties().getProperty(SoapConnectorProperties.WS_SECURITY_TYPE);
                if (wsSecurityType != null && wsSecurityType.length() > 0) {

                    // if this is WS-security then we need to find sub-category of it
                    WSSecurityToken rootToken = new WSSecurityToken(env, trustedPayloadHandler);
                    WSSecurityToken nextToken =  rootToken;
                    
                    StringTokenizer st = new StringTokenizer(wsSecurityType);                    
                    while (st.hasMoreTokens()) {
                        // get the next auth type specified.
                        String authType = st.nextToken();

                        if (authType.equalsIgnoreCase(USERNAME_TOKEN_PROFILE_CLEAR_TEXT)) {
                            nextToken = nextToken.setNextToken(new UsernameTokenProfile(env, trustedPayloadHandler, false));
                        }
                        else if (authType.equalsIgnoreCase(USERNAME_TOKEN_PROFILE_DIGEST)) {
                            nextToken = nextToken.setNextToken( new UsernameTokenProfile(env, trustedPayloadHandler, true));
                        }
                        else if (authType.equalsIgnoreCase(SAML_TOKEN_UNSIGNED)) {
                            nextToken = nextToken.setNextToken( new SAMLTokenProfile(env, trustedPayloadHandler, false));
                        }
                        else if (authType.equalsIgnoreCase(SAML_TOKEN_SIGNED)) {
                            nextToken = nextToken.setNextToken( new SAMLTokenProfile(env, trustedPayloadHandler, true));
                        }
                        else if (authType.equalsIgnoreCase(SIGNATURE)) {
                            nextToken = nextToken.setNextToken( new SignatureProfile(env, trustedPayloadHandler));
                        }
                        else if (authType.equalsIgnoreCase(TIMESTAMP)) {
                            nextToken = nextToken.setNextToken( new TimestampProfile(env, trustedPayloadHandler));
                        }
                        else if (authType.equalsIgnoreCase(ENCRYPT)) {
                            nextToken = nextToken.setNextToken( new EncryptProfile(env, trustedPayloadHandler));
                        }
                        else {
                            throw new RuntimeException(XMLSourcePlugin.Util.getString("No_such_ws_security_type", new Object[] {authType})); //$NON-NLS-1$                    
                        }
                    }
                    return rootToken;
                }
                throw new RuntimeException(XMLSourcePlugin.Util.getString("No_ws_security_type")); //$NON-NLS-1$    
            }
            else {
                throw new RuntimeException(XMLSourcePlugin.Util.getString("No_such_auth_type", new Object[] {securityType})); //$NON-NLS-1$                    
            }                        
        }
        return new NoProvider(env, trustedPayloadHandler);
    }
    
    SecurityToken(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler) {
        this.env = env;
        if (null != trustedPayloadHandler) {
        	String tempPassword = trustedPayloadHandler.getPassword();
        	String tempUsername = trustedPayloadHandler.getUser();
        	if(null != tempPassword && null != tempUsername) {
        		password = tempPassword;
        		username = tempUsername;
        	} else {
        		setCredentialsFromEnv();
        	}
        } else {
        	setCredentialsFromEnv();
        }
    }

	private void setCredentialsFromEnv() {
		username = this.env.getProperties().getProperty(SoapConnectorProperties.USERNAME);
		password = this.env.getProperties().getProperty(SoapConnectorProperties.PASSWORD);
	}
    
    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }
    
    String getProperty(String name) {
        return this.env.getProperties().getProperty(name);
    }
    
    String getTrustType() {
        String type = this.env.getProperties().getProperty(SoapConnectorProperties.TRUST_TYPE);
        if (type != null) {
            if (type.equalsIgnoreCase(ISSUER_SERIAL)) {
                return ISSUER_SERIAL;
            }
            else if (type.equalsIgnoreCase(DIRECT_REFERENCE)) {
                return DIRECT_REFERENCE;
            }
        }
        throw new RuntimeException(XMLSourcePlugin.Util.getString("no_trust_type_defined")); //$NON-NLS-1$
    }
    
    public abstract void handleSecurity(Call stub);
}

/**
 * A marker class to differentiate between WS-Security and non - WS-Secuirty like
 * HTTP Based one  
 */
class WSSecurityToken extends SecurityToken implements CallbackHandler {
    WSSecurityToken nextToken;
    
    public WSSecurityToken(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler) {
        super(env, trustedPayloadHandler);
    }
    
    @Override
	public void handleSecurity(Call call) {
        addSecurity(call);
        if (nextToken != null) {
            this.nextToken.handleSecurity(call);
        }
    }

    WSSecurityToken getNextToken() {
        return nextToken;
    }   
    
    WSSecurityToken setNextToken(WSSecurityToken token) {
        this.nextToken = token;
        return token;
    }       
    
    // this needs to be extended by everybody toadd specific
    // type of secuirty; should have been abstract but to minimize the code
    // choose to have empty method.
    void addSecurity(Call call) {
        // nothing to do.
    }
    
    void setAction(Call call, String action) {
        String prev = (String)call.getProperty(WSHandlerConstants.ACTION);
        if (prev == null || prev.length() == 0) {
            call.setProperty(WSHandlerConstants.ACTION, action);
        }
        else {
            call.setProperty(WSHandlerConstants.ACTION, prev+" "+action); //$NON-NLS-1$
        }
    }
    
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof WSPasswordCallback) {
                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
                    pc.setPassword(getPassword());
            } else {
                throw new UnsupportedCallbackException(callbacks[i], "unrecognized_callback"); //$NON-NLS-1$
            }
        }            
    }     
}

/**
 * defines that there is no security provider 
 */
class NoProvider extends SecurityToken{
    public NoProvider(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler){
        super(env, trustedPayloadHandler);
    }
    @Override
	public void handleSecurity(Call stub) {
    }
}

/**
 * This uses the HTTP Basic authentication; So the user credentials are sent over 
 * in HTTP Headers.  Uses Basic-Relam.
 */
class HTTPBasic extends SecurityToken{
    public HTTPBasic(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler){
        super(env, trustedPayloadHandler);
    }
    @Override
	public void handleSecurity(Call stub) {
        stub.setUsername(getUsername());
        stub.setPassword(getPassword());
        XMLSourcePlugin.logDetail(this.env.getLogger(), "using_http_basic"); //$NON-NLS-1$
    }
}

/**
 * Timestamp Profile using WSS4J
 */
class TimestampProfile extends WSSecurityToken {

    public TimestampProfile(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler) {
        super(env, trustedPayloadHandler);
        XMLSourcePlugin.logDetail(this.env.getLogger(), "using_timestamp_profile"); //$NON-NLS-1$
    }
    @Override
	public void addSecurity(Call call) {
        setAction(call, WSHandlerConstants.TIMESTAMP);
        
        // How long ( in seconds ) message is valid since send.
        call.setProperty(WSHandlerConstants.TTL_TIMESTAMP,"60"); //$NON-NLS-1$
        // if you want to use millisecond precision use this
        //properties.setProperty(WSHandlerConstants.TIMESTAMP_PRECISION,"true");        
    }    
}

/**
 * This class uses the WS-Security using standard OASIS Web Services Security
 * implemented by apache "WSS4J" Implements "Username Token Profile"
 */
class UsernameTokenProfile extends WSSecurityToken {        
    boolean encryptedPassword = false;
    
    public UsernameTokenProfile(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler, boolean encryptedPassword){
        super(env, trustedPayloadHandler);
        this.encryptedPassword = encryptedPassword;
        XMLSourcePlugin.logDetail(this.env.getLogger(), "using_username_profile"); //$NON-NLS-1$
    }
    
    @Override
	public void addSecurity(Call call) {
        setAction(call, WSHandlerConstants.USERNAME_TOKEN);
        call.setProperty(WSHandlerConstants.USER, getUsername());
        if (this.encryptedPassword) {
            call.setProperty(UsernameToken.PASSWORD_TYPE, WSConstants.PW_DIGEST);
        }
        else {
            call.setProperty(UsernameToken.PASSWORD_TYPE, WSConstants.PW_TEXT);
        }        
        call.setProperty(WSHandlerConstants.PW_CALLBACK_REF, this);
    }

    @Override
	public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (int i = 0; i < callbacks.length; i++) {
            if (callbacks[i] instanceof WSPasswordCallback) {
                WSPasswordCallback pc = (WSPasswordCallback)callbacks[i];
                if (this.encryptedPassword) {
                    pc.setPassword(encrypt(getPassword()));
                }
                else {
                    pc.setPassword(getPassword());
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
}

class EncryptProfile extends WSSecurityToken {            
    public EncryptProfile(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler){
        super(env, trustedPayloadHandler);
        XMLSourcePlugin.logDetail(this.env.getLogger(), "using_encrypt_profile"); //$NON-NLS-1$
    }
    
    @Override
	public void addSecurity(Call call) {
        setAction(call, WSHandlerConstants.ENCRYPT);
        String user = getProperty(SoapConnectorProperties.ENCRYPTION_USER);
        if (user == null || user.length() == 0) {            
            call.setProperty(WSHandlerConstants.ENCRYPTION_USER, getUsername());
        }
        else {
            call.setProperty(WSHandlerConstants.ENCRYPTION_USER, user);
        }
        call.setProperty(WSHandlerConstants.USER, getUsername());
        
        //Configuration of public key used to encrypt message goes to properties file.
        String encryptionProp = getProperty(SoapConnectorProperties.ENCRYPTION_PROPERTY_FILE);
        if (encryptionProp == null || encryptionProp.length() == 0) {
            throw new RuntimeException(XMLSourcePlugin.Util.getString("no_encryption_property_file")); //$NON-NLS-1$ 
        }
        call.setProperty(WSHandlerConstants.ENC_PROP_FILE, encryptionProp);
        call.setProperty(WSHandlerConstants.ENC_KEY_ID, "SKIKeyIdentifier"); //$NON-NLS-1$
    }
}

/**
 * Digital signature profile using WSS4J
 */                
class SignatureProfile extends WSSecurityToken {

    public SignatureProfile(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler) {
        super(env, trustedPayloadHandler);
        XMLSourcePlugin.logDetail(this.env.getLogger(), "using_signature_profile"); //$NON-NLS-1$
    }
    
    @Override
	public void addSecurity(Call call) {
        setAction(call, WSHandlerConstants.SIGNATURE);
        call.setProperty(WSHandlerConstants.USER, getUsername());
        call.setProperty(WSHandlerConstants.PW_CALLBACK_REF, this);
        
        String cryptoFile = getProperty(SoapConnectorProperties.SIGNATURE_PROPERTY_FILE);
        if (cryptoFile == null || cryptoFile.length() == 0) {
            throw new RuntimeException(XMLSourcePlugin.Util.getString("no_crypto_property_file")); //$NON-NLS-1$ 
        }
        call.setProperty(WSHandlerConstants.SIG_PROP_FILE, cryptoFile);
        call.setProperty(WSHandlerConstants.SIG_KEY_ID, getTrustType()); 
        call.setProperty(WSHandlerConstants.SIGNATURE_PARTS, "{}{http://schemas.xmlsoap.org/soap/envelope/}Body;STRTransform"); //$NON-NLS-1$        
    }   
}

/**
 * SAML Profile based authentication using WSS4J. 
 */
class SAMLTokenProfile extends WSSecurityToken {
    boolean signed = false;
    
    public SAMLTokenProfile(ConnectorEnvironment env, TrustedPayloadHandler trustedPayloadHandler, boolean signed){
        super(env, trustedPayloadHandler);
        this.signed = signed;
    }

    @Override
	public void addSecurity(Call call) {
        
        if (signed) {
            setAction(call, WSHandlerConstants.SAML_TOKEN_SIGNED);
            
            String cryptoFile = getProperty(SoapConnectorProperties.SIGNATURE_PROPERTY_FILE);
            if (cryptoFile == null || cryptoFile.length() == 0) {
                throw new RuntimeException(XMLSourcePlugin.Util.getString("no_crypto_property_file")); //$NON-NLS-1$ 
            }
            call.setProperty(WSHandlerConstants.SIG_PROP_FILE, cryptoFile);
            call.setProperty(WSHandlerConstants.SIG_KEY_ID, getTrustType()); 
        }
        else {
            setAction(call, WSHandlerConstants.SAML_TOKEN_UNSIGNED);   
        }
        
        // Set user name and password; for trust type "keyholder" these are required. 
        if (getUsername() != null) {
            call.setProperty(WSHandlerConstants.USER, getUsername());
        }
        if (getPassword() != null) {
            call.setProperty(WSHandlerConstants.PW_CALLBACK_REF, this);
        }
        
        // set the SAML Properties file
        String samlPropertyFile = getProperty(SoapConnectorProperties.SAML_PROPERTY_FILE);
        if (samlPropertyFile == null || samlPropertyFile.length() == 0) {
            throw new RuntimeException(XMLSourcePlugin.Util.getString("no_saml_property_file")); //$NON-NLS-1$ 
        }
        call.setProperty(WSHandlerConstants.SAML_PROP_FILE, samlPropertyFile);                
    }            
}