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
package org.teiid.jboss;

import java.security.AccessController;
import java.security.Principal;
import java.security.PrivilegedAction;
import java.security.acl.Group;
import java.util.HashMap;
import java.util.Map;

import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.jboss.security.SecurityContextAssociation;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.vault.SecurityVaultException;
import org.jboss.security.vault.SecurityVaultUtil;
import org.picketbox.datasource.security.AbstractPasswordCredentialLoginModule;

/**
 * A simple login module passes the principal making the connection request 
 * to EIS, as pass-through without any validation.
 * 
 */

@SuppressWarnings("unchecked")
public class PassthroughIdentityLoginModule extends AbstractPasswordCredentialLoginModule {
   private String userName;
   private char[] password;
   private Subject callerSubject;
   private boolean addPrincipal = true;
   private HashMap<String, Object> properties = new HashMap<String, Object>();

   @Override
   public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
      super.initialize(subject, handler, sharedState, options);

      userName = (String) options.get("username"); //$NON-NLS-1$

      String pass = (String) options.get("password");//$NON-NLS-1$
        if (pass != null) {
            if (SecurityVaultUtil.isVaultFormat(pass)) {
                try {
                    pass = SecurityVaultUtil.getValueAsString(pass);
                } catch (SecurityVaultException e) {
                    throw new RuntimeException(e);
                }
                password = pass.toCharArray();
            } else {
                password = pass.toCharArray();
            }
        }
        this.properties.putAll(options);
   }

    @Override
    public boolean login() throws LoginException {

        String username = userName;
        try {
            Principal user = getPrincipal();
            this.callerSubject = getSubject();
            this.addPrincipal = false;
            
            if (user != null) {
                username = user.getName();
            }            
        } catch (Throwable e) {
            throw new LoginException(e.getMessage());
        }

        // Update userName so that getIdentity is consistent
        this.userName = username;
        if (super.login() == true) {
            return true;
        }

        // Put the principal name into the sharedState map
        sharedState.put("javax.security.auth.login.name", username); //$NON-NLS-1$
        super.loginOk = true;
        return true;
   }

   @Override
   public boolean commit() throws LoginException {
      // Put the principal name into the sharedState map
      sharedState.put("javax.security.auth.login.name", userName); //$NON-NLS-1$
      
      if (this.addPrincipal) {
          subject.getPrincipals().add(getIdentity());
          
          // Add the PasswordCredential
          if (this.password != null) {
              PasswordCredential cred = new PasswordCredential(userName, password);
              SecurityActions.addCredentials(subject, cred);
          }          
      }
            
      if (this.callerSubject != null) {
          makeCopy(this.callerSubject, this.subject);
      }
      addPrivateCredential(this.subject, this.properties);
      return true;
   }
   
   @Override
   protected Principal getIdentity() {
      Principal principal = new SimplePrincipal(userName);
      return principal;
   }   
   
   @Override
   protected Group[] getRoleSets() throws LoginException {
       return new Group[]{};    
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
   
   static Object makeCopy(final Subject from, final Subject to) {
       if (System.getSecurityManager() == null) {
           copy(from, to);
           return null;
       }
       
       return AccessController.doPrivileged(new PrivilegedAction<Object>() { 
           public Object run() {
               copy(from, to);
               return null;
           }
       });        
   }   
   
   static void copy (final Subject from, final Subject to) {
       for(Principal p:from.getPrincipals()) {
           to.getPrincipals().add(p);
       }
       
       for (Object obj: from.getPrivateCredentials()) {
           to.getPrivateCredentials().add(obj);
       }
       
       for (Object obj: from.getPublicCredentials()) {
           to.getPublicCredentials().add(obj);
       }
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
}
