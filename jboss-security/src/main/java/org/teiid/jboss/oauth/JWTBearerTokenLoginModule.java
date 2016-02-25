/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss.oauth;

import java.io.IOException;
import java.security.Key;
import java.security.PrivateKey;
import java.security.Signature;
import java.text.MessageFormat;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils;
import org.apache.cxf.rs.security.oauth2.common.ClientAccessToken;
import org.apache.cxf.rs.security.oauth2.grants.jwt.JwtBearerGrant;
import org.jboss.security.JBossJSSESecurityDomain;
import org.teiid.core.util.Base64;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;

public class JWTBearerTokenLoginModule extends OAuth20LoginModule {
    private String scope;
    private String issuer;
    private String audience;
    private String subject;
    
    private String keystoreType;
    private String keystorePassword;
    private String keystoreURL;
    private String certificateAlias;
    private String certificatePassword;
    private String algorithamName;
    private static JBossJSSESecurityDomain securityDomain;
    
    @Override
    public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
       super.initialize(subject, handler, sharedState, options);
       
       this.scope = (String) options.get("scope"); //$NON-NLS-1$
       this.issuer = (String) options.get("jwt-issuer"); //$NON-NLS-1$
       this.audience = (String) options.get("jwt-audience"); //$NON-NLS-1$
       this.subject= (String) options.get("jwt-subject"); //$NON-NLS-1$
       
       this.keystoreType = (String) options.get("keystore-type"); //$NON-NLS-1$
       this.keystorePassword = (String) options.get("keystore-password"); //$NON-NLS-1$
       this.keystoreURL = (String) options.get("keystore-url"); //$NON-NLS-1$
       this.certificateAlias = (String) options.get("certificate-alias"); //$NON-NLS-1$
       this.certificatePassword = (String) options.get("certificate-password"); //$NON-NLS-1$
       this.algorithamName = (String) options.get("signature-algorithm-name"); //$NON-NLS-1$      
    }
    
    @Override
    public boolean login() throws LoginException {
        this.callerSubject = getSubject();
        this.callerPrincipal = getPrincipal();
        
        final String assertion = getJWTAssertion();
        if (assertion == null) {
            return false;
        }
        
        OAuth20CredentialImpl cred = new OAuth20CredentialImpl() {
            protected ClientAccessToken getAccessToken() {
                OAuthClientUtils.Consumer consumer = new OAuthClientUtils.Consumer(getClientId(), getClientSecret());
                WebClient client = WebClient.create(getAccessTokenURI());
                JwtBearerGrant grant = null;
                if (scope != null) {
                    grant = new JwtBearerGrant(assertion, true, scope);
                }
                else {
                    grant = new JwtBearerGrant(assertion, true);
                }
                return OAuthClientUtils.getAccessToken(client, consumer, grant, null, false);
            }            
        };
        cred.setClientId(getClientId());
        cred.setClientSecret(getClientSecret());
        cred.setAccessTokenURI(getAccessTokenURI());
        setCredential(cred);
        return super.login();
    }

    /**
     * override if needed
     * @return
     */
    protected String getJWTAssertion() throws LoginException {
        String header = "{\"alg\":\"RS256\"}";
        String claimTemplate = "'{'\"iss\": \"{0}\", \"sub\": \"{1}\", \"aud\": \"{2}\", \"exp\": \"{3}\"'}'";

        StringBuffer token = new StringBuffer();

        try {
            // Encode the JWT Header and add it to our string to sign
            token.append(Base64.encodeUrlSafe(header.getBytes("UTF-8")));

            // Separate with a period
            token.append(".");

            // Create the JWT Claims Object
            String[] claimArray = new String[4];
            claimArray[0] = this.issuer == null ? getClientId() : this.issuer;
            claimArray[1] = this.subject == null ? this.callerPrincipal.getName(): this.subject;
            claimArray[2] = this.audience;
            claimArray[3] = Long.toString((System.currentTimeMillis() / 1000) + 120);
            MessageFormat claims = new MessageFormat(claimTemplate);
            String payload = claims.format(claimArray);

            // Add the encoded claims object
            token.append(Base64.encodeUrlSafe(payload.getBytes("UTF-8")));

            String password = this.certificatePassword == null ? this.keystorePassword
                    : this.certificatePassword;
            loadKeystore(this.keystoreURL, this.keystorePassword, this.keystoreType, password);
            
            // Sign the JWT Header + "." + JWT Claims Object
            Key key = securityDomain.getKey(this.certificateAlias, password);
            Signature signature = Signature.getInstance(this.algorithamName == null?"SHA256withRSA":this.algorithamName);
            signature.initSign((PrivateKey) key);
            signature.update(token.toString().getBytes("UTF-8"));
            String signedPayload = Base64.encodeUrlSafe(signature.sign());

            // Separate with a period
            token.append(".");

            // Add the encoded signature
            token.append(signedPayload);

            return token.toString();
        } catch (Exception e) {
            LogManager.logDetail(LogConstants.CTX_SECURITY, e);
            throw new LoginException(e.getMessage());
        }
    }

    private static void loadKeystore(String keystoreURL,
            String keystorePassword, String keystoreType, String password)
            throws Exception, IOException {
        if (securityDomain == null) {
            securityDomain = new JBossJSSESecurityDomain("JWTBearer");
            securityDomain.setKeyStorePassword(keystorePassword);
            securityDomain.setKeyStoreType(keystoreType == null ? "JKS": keystoreType);
            securityDomain.setKeyStoreURL(keystoreURL);
            securityDomain.setServiceAuthToken(password);
            securityDomain.reloadKeyAndTrustStore();
        }
    }
}
