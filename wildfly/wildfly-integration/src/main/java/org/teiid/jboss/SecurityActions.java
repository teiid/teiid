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

import javax.resource.spi.security.PasswordCredential;
import javax.security.auth.Subject;

import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.security.SecurityHelper;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

class SecurityActions {

    private static final ThreadLocal<SecurityIdentity> securityIdentityThreadLocal;

    static {
        String saflag = System.getProperty("org.jboss.security.SecurityAssociation.ThreadLocal", "false");
        String scflag = System.getProperty("org.jboss.security.context.ThreadLocal", "false");
        boolean useThreadLocal = Boolean.parseBoolean(saflag) || Boolean.parseBoolean(scflag);
        if (useThreadLocal) {
            securityIdentityThreadLocal = new ThreadLocal<>();
        } else {
            securityIdentityThreadLocal = new InheritableThreadLocal<>();
        }
    }

    static void setSecurityIdentity(final SecurityIdentity sc)
       {
          AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
              securityIdentityThreadLocal.set(sc);
             return null;
          });
       }

       static SecurityIdentity getSecurityIdentity()
       {
          return AccessController.doPrivileged((PrivilegedAction<SecurityIdentity>) securityIdentityThreadLocal::get);
       }

       static void clearSecurityIdentity()
       {
          AccessController.doPrivileged((PrivilegedAction<SecurityIdentity>) () -> {
              securityIdentityThreadLocal.remove();
              return null;
          });
       }

       static SecurityIdentity createSecurityIdentity(final Principal p, final Object cred, final String securityDomainName, final SecurityHelper securityHelper)
       {
           return AccessController.doPrivileged((PrivilegedAction<SecurityIdentity>) () -> {
               SecurityIdentity securityIdentity = null;
               if (securityHelper instanceof JBossSecurityHelper) {
                   JBossSecurityHelper jBossSecurityHelper = (JBossSecurityHelper) securityHelper;
                   SecurityDomain securityDomain = jBossSecurityHelper.getSecurityDomain(securityDomainName);
                   try {
                       securityIdentity = securityDomain.authenticate(p, new PasswordGuessEvidence(cred.toString().toCharArray()));
                   } catch (RealmUnavailableException e) {
                       LogManager.logError(LogConstants.CTX_SECURITY, "Failed to authenticate '" + p.getName() /*+ "' with creds '" + cred + "'"*/);
                   }
/*
                    if (securityIdentity == null) {
                        try {
                            securityIdentity = securityDomain.createAdHocIdentity(p);
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
*/
               }
               return securityIdentity;
           });
       }

       static class AddCredentialsAction implements PrivilegedAction<Object>
       {
          Subject subject;
          PasswordCredential cred;
          AddCredentialsAction(Subject subject, PasswordCredential cred)
          {
             this.subject = subject;
             this.cred = cred;
          }
          public Object run()
          {
             subject.getPrivateCredentials().add(cred);
             return null;
          }
       }

       static void addCredentials(Subject subject, PasswordCredential cred)
       {
          AddCredentialsAction action = new AddCredentialsAction(subject, cred);
          AccessController.doPrivileged(action);
       }
}
