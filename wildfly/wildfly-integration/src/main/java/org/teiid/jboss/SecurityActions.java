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

import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityContextAssociation;
import org.jboss.security.SecurityContextFactory;

class SecurityActions {
       static void setSecurityContext(final SecurityContext sc)
       {
          AccessController.doPrivileged(new PrivilegedAction<Object>()
          {
             public Object run()
             {
                SecurityContextAssociation.setSecurityContext(sc);
                return null;
             }
          });
       }

       static SecurityContext getSecurityContext()
       {
          return AccessController.doPrivileged(new PrivilegedAction<SecurityContext>()
          {
             public SecurityContext run()
             {
                return SecurityContextAssociation.getSecurityContext();
             }
          });
       }

       static SecurityContext clearSecurityContext()
       {
          return AccessController.doPrivileged(new PrivilegedAction<SecurityContext>()
          {
             public SecurityContext run()
             {
                SecurityContextAssociation.clearSecurityContext();
                return null;
             }
          });
       }

       static SecurityContext createSecurityContext(final Principal p, final Object cred, final Subject subject, final String securityDomain)
       {
            return AccessController.doPrivileged(new PrivilegedAction<SecurityContext>() {
                public SecurityContext run() {
                    SecurityContext sc;
                    try {
                        sc = SecurityContextFactory.createSecurityContext(p, cred, subject, securityDomain);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    return sc;
                }
            });
       }

       static class AddCredentialsAction implements PrivilegedAction
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
