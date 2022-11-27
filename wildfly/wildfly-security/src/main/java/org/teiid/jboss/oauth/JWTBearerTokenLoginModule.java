/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
import org.apache.cxf.rs.security.oauth2.client.Consumer;
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
                Consumer consumer = new Consumer(getClientId(), getClientSecret());
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
