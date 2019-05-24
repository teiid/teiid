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

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSName;
import org.ietf.jgss.Oid;
import org.jboss.security.SecurityContextAssociation;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.vault.SecurityVaultException;
import org.jboss.security.vault.SecurityVaultUtil;
import org.picketbox.datasource.security.AbstractPasswordCredentialLoginModule;
import org.teiid.OAuthCredentialContext;

/**
 * A simple login module passes the principal making the connection request
 * to EIS, as pass-through without any validation.
 *
 */

@SuppressWarnings("unchecked")
public class PassthroughIdentityLoginModule extends AbstractPasswordCredentialLoginModule {
   /**
    * Module option to specify if any {@link GSSCredential} being added to the
    * {@link Subject} should be wrapped to prevent disposal.
    *
    * Has no effect if a {@link GSSCredential} is not being added to the
    * {@link Subject}.
    *
    * Defaults to false.
    */
   private static final String WRAP_GSS_CREDENTIAL = "wrapGSSCredential";
   private String userName;
   private char[] password;
   private Subject callerSubject;
   private boolean addPrincipal = true;
   private HashMap<String, Object> properties = new HashMap<String, Object>();
   private boolean wrapGssCredential;
   private Subject intermediateSubject;
   private GSSCredential storedCredential;

   @Override
   public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
      super.initialize(subject, handler, sharedState, options);

      this.userName = (String) options.get("username"); //$NON-NLS-1$

      String pass = (String) options.get("password");//$NON-NLS-1$
        if (pass != null) {
            if (SecurityVaultUtil.isVaultFormat(pass)) {
                try {
                    pass = SecurityVaultUtil.getValueAsString(pass);
                } catch (SecurityVaultException e) {
                    throw new RuntimeException(e);
                }
                this.password = pass.toCharArray();
            } else {
                this.password = pass.toCharArray();
            }
        }
        this.properties.putAll(options);
        this.wrapGssCredential = Boolean.parseBoolean((String) options.get(WRAP_GSS_CREDENTIAL));
        log.tracef("wrapGssCredential=%b", wrapGssCredential);
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
          GSSCredential rawCredential = getGssCredential(this.callerSubject);
          if (rawCredential != null) {
              log.trace("Kerberos passthough mechanism in works");
              this.storedCredential = wrapGssCredential ? wrapCredential(rawCredential) : rawCredential;
              this.intermediateSubject = GSSUtil.createGssSubject(rawCredential, storedCredential);
              if (this.intermediateSubject == null){
                  throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50108));
              }
              log.tracef("created a subject from deletegate credential");
              makeCopy(intermediateSubject, this.subject);
              log.tracef("Copied contents of temporary Subject to Subject from the LoginContext");
              addPrivateCredential(this.subject, storedCredential);
              log.trace("Also add the GSSCredential to the Subject");
          } else {
              makeCopy(this.callerSubject, this.subject);
          }
      }

      addPrivateCredential(this.subject, this.properties);
      log.trace("Adding module option properties as private credential");

      // if oauth credential available in calling context then add the OAuthCredential.
      if (OAuthCredentialContext.getCredential() != null) {
          addPrivateCredential(this.subject, OAuthCredentialContext.getCredential());
          log.trace("Adding OAuth credential as private credential");
      }
      return true;
   }

   @Override
   public boolean logout() throws LoginException {
       if (System.getSecurityManager() == null) {
           if (storedCredential != null) {
               removePrivateCredential(subject, storedCredential);
               log.trace("Remove GSSCredential to the Subject");
           }
           removePrivateCredential(subject, properties);
           clearSubjectContents(subject, intermediateSubject != null?intermediateSubject:callerSubject);
           log.trace("Clear Subject contents");
           return true;
       }

       return AccessController.doPrivileged(new PrivilegedAction<Boolean>() {
           public Boolean run() {
               if (storedCredential != null) {
                   removePrivateCredential(subject, storedCredential);
                   log.trace("Remove GSSCredential to the Subject");
               }
               removePrivateCredential(subject, properties);
               clearSubjectContents(subject, intermediateSubject != null?intermediateSubject:callerSubject);
               log.trace("Clear Subject contents");
               return true;
           }
       });
   }

   private GSSCredential getGssCredential(Subject subject) {
       for(Object obj:subject.getPrivateCredentials()) {
           if (obj instanceof GSSCredential) {
               return (GSSCredential)obj;
           }
       }
       return null;
   }

   void clearSubjectContents(Subject toSubtract, Subject from) {
       from.getPrincipals().removeAll(toSubtract.getPrincipals());
       from.getPublicCredentials().removeAll(toSubtract.getPublicCredentials());
       from.getPrivateCredentials().removeAll(toSubtract.getPrivateCredentials());
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

   static void removePrivateCredential(final Subject subject, final Object obj) {
       if (System.getSecurityManager() == null) {
           subject.getPrivateCredentials().remove(obj);
       }
       else {
       AccessController.doPrivileged(new PrivilegedAction<Object>() {
           public Object run() {
               subject.getPrivateCredentials().remove(obj);
               return null;
           }
       });
       }
   }

   private static GSSCredential wrapCredential(final GSSCredential credential) {
       return new GSSCredential() {

           @Override
           public int getUsage(Oid mech) throws GSSException {
               return credential.getUsage(mech);
           }

           @Override
           public int getUsage() throws GSSException {
               return credential.getUsage();
           }

           @Override
           public int getRemainingLifetime() throws GSSException {
               return credential.getRemainingLifetime();
           }

           @Override
           public int getRemainingInitLifetime(Oid mech) throws GSSException {
               return credential.getRemainingInitLifetime(mech);
           }

           @Override
           public int getRemainingAcceptLifetime(Oid mech) throws GSSException {
               return credential.getRemainingAcceptLifetime(mech);
           }

           @Override
           public GSSName getName(Oid mech) throws GSSException {
               return credential.getName(mech);
           }

           @Override
           public GSSName getName() throws GSSException {
               return credential.getName();
           }

           @Override
           public Oid[] getMechs() throws GSSException {
               return credential.getMechs();
           }

           @Override
           public void dispose() throws GSSException {
               // Prevent disposal of our credential.
           }

           @Override
           public void add(GSSName name, int initLifetime, int acceptLifetime, Oid mech, int usage) throws GSSException {
               credential.add(name, initLifetime, acceptLifetime, mech, usage);
           }
       };
   }
}
