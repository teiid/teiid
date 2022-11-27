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

/**
 * Login module to capture OAuth 1.0a profile credential for web service resource-adapter.
 * Users either need to provide all the options or extend this login module to provide
 * all necessary options at runtime.
 */
public class OAuth10LoginModule extends AbstractPasswordCredentialLoginModule {
    private String consumerKey;
    private String consumerSecret;
    private String accessKey;
    private String accessSecret;
    protected OAuthCredential credential;
    protected Subject callerSubject;
    protected Principal callerPrincipal;

    @Override
    public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
       super.initialize(subject, handler, sharedState, options);

       this.consumerKey = (String) options.get("consumer-key"); //$NON-NLS-1$
       this.consumerSecret = (String) options.get("consumer-secret"); //$NON-NLS-1$

       this.accessKey = (String) options.get("access-key"); //$NON-NLS-1$
       this.accessSecret = (String) options.get("access-secret"); //$NON-NLS-1$
    }

    @Override
    public boolean login() throws LoginException {
        this.callerSubject = getSubject();
        this.callerPrincipal = getPrincipal();

        if (getCredential() == null) {
            if (getConsumerKey() == null || getConsumerSecret() == null ||
                    getAccessKey() == null || getAccessSecret() == null) {
                super.loginOk = false;
                return false;
            }
            // build credential from options.
            OAuth10CredentialImpl cred = new OAuth10CredentialImpl();
            cred.setConsumerKey(getConsumerKey());
            cred.setConsumerSecret(getConsumerSecret());
            cred.setAccessToken(getAccessKey());
            cred.setAccessSecret(getAccessSecret());
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

    public String getConsumerKey() {
        return consumerKey;
    }

    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public void setAccessSecret(String accessSecret) {
        this.accessSecret = accessSecret;
    }

    public OAuthCredential getCredential() {
        return credential;
    }

    public void setCredential(OAuthCredential credential) {
        this.credential = credential;
    }
}
