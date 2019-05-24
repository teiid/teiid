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

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.acl.Group;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.jboss.security.SimplePrincipal;
import org.jboss.security.SecurityContextAssociation;
import org.picketbox.datasource.security.AbstractPasswordCredentialLoginModule;
import org.teiid.OAuthCredential;
import org.teiid.OAuthCredentialContext;

/**
 * Login module to capture OAuth 2.0 profile credential for web service resource-adapter.
 * Users either need to provide all the options or extend this login module to provide
 * all necessary options at runtime.
 */
public class OAuth20LoginModule extends AbstractPasswordCredentialLoginModule {
    private String clientId;
    private String clientSecret;
    private String refreshToken;
    private String accessTokenURI;
    private String accessToken;
    protected OAuthCredential credential;
    protected Subject callerSubject;
    protected Principal callerPrincipal;

    @Override
    public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
       super.initialize(subject, handler, sharedState, options);

       this.clientId = (String) options.get("client-id"); //$NON-NLS-1$
       this.clientSecret = (String) options.get("client-secret"); //$NON-NLS-1$

       this.refreshToken = (String) options.get("refresh-token"); //$NON-NLS-1$
       this.accessTokenURI = (String) options.get("access-token-uri"); //$NON-NLS-1$
        this.accessToken = (String) options.get("access-token"); //$NON-NLS-1$
    }

    @Override
    public boolean login() throws LoginException {
        this.callerSubject = getSubject();
        this.callerPrincipal = getPrincipal();

        if (getCredential() == null) {
            if (getClientId() == null || getClientSecret() == null ||
                    (getAccessTokenURI() == null && getAccessToken() == null)
                    || (getRefreshToken() == null && getAccessToken() == null)) {
                super.loginOk = false;
                return false;
            }

            // build credential from options.
            OAuth20CredentialImpl cred = new OAuth20CredentialImpl();
            cred.setClientId(getClientId());
            cred.setClientSecret(getClientSecret());
            cred.setRefreshToken(getRefreshToken());
            cred.setAccessTokenURI(getAccessTokenURI());
            cred.setAccessTokenString(getAccessToken());
            setCredential(cred);
        }

        super.loginOk = true;
        return true;
   }

    @Override
    protected Principal getIdentity() {
        if (this.callerPrincipal != null) {
            return this.callerPrincipal;
        }
        return new SimplePrincipal("oauth-user"); //$NON-NLS-1$
    }

    @Override
    protected Group[] getRoleSets() throws LoginException {
        return new Group[]{};
    }

    @Override
    public boolean commit() throws LoginException {
       subject.getPrincipals().add(getIdentity());
       addPrivateCredential(this.subject, getCredential());
       return true;
    }

    static void addPrivateCredential(final Subject subject, final Object obj) {
        if (System.getSecurityManager() == null) {
            subject.getPrivateCredentials().add(obj);
        }
        else {
        AccessController.doPrivileged(new PrivilegedAction<Object>() {
            public Object run() {
                subject.getPrivateCredentials().add(obj);
                return null;
            }
        });
        }
    }

    static Principal getPrincipal() {
        if (System.getSecurityManager() == null) {
            return SecurityContextAssociation.getPrincipal();
        }

        return AccessController.doPrivileged(new PrivilegedAction<Principal>() {
            public Principal run() {
                return SecurityContextAssociation.getPrincipal();
            }
        });
    }

    static Subject getSubject() {
        if (System.getSecurityManager() == null) {
            return SecurityContextAssociation.getSubject();
        }

        return AccessController.doPrivileged(new PrivilegedAction<Subject>() {
            public Subject run() {
                return SecurityContextAssociation.getSubject();
            }
        });
    }

    public OAuthCredential getCredential() {
        if (this.credential != null) {
            return this.credential;
        }
        return OAuthCredentialContext.getCredential();
    }

    public void setCredential(OAuthCredential credential) {
        this.credential = credential;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getAccessTokenURI() {
        return accessTokenURI;
    }

    public void setAccessTokenURI(String accessTokenURI) {
        this.accessTokenURI = accessTokenURI;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}
