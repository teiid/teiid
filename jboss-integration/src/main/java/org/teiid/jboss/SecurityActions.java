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
