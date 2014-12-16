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
