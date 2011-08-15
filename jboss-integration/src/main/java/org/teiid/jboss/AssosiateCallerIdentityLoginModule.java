/*
 * JBoss, Home of Professional Open Source
 * Copyright 2007, JBoss Inc., and individual contributors as indicated
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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

import java.security.Principal;
import java.security.acl.Group;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.jboss.security.SecurityContext;
import org.jboss.security.SubjectInfo;
import org.jboss.security.auth.spi.AbstractServerLoginModule;

/**
 * This login modules simply takes the subject in the current context and adds
 * its principle to shared state. This is same as CallerIdentityLoginModule,
 * just it does not extend the AbstractPasswordCredentialLoginModule
 */
public class AssosiateCallerIdentityLoginModule extends AbstractServerLoginModule {

	private Principal principal;
	
	public void initialize(Subject subject, CallbackHandler handler,
			Map sharedState, Map options) {
		super.initialize(subject, handler, sharedState, options);
	}

	/**
	 * Performs the login association between the caller and the resource for a
	 * 1 to 1 mapping. This acts as a login propagation strategy and is useful
	 * for single-sign on requirements
	 * 
	 * @return True if authentication succeeds
	 * @throws LoginException
	 */
	public boolean login() throws LoginException {

		SecurityContext sc = SecurityActions.getSecurityContext();
		SubjectInfo si = sc.getSubjectInfo();
		Subject subject = si.getAuthenticatedSubject();
		
		Set<Principal> principals = subject.getPrincipals();
		this.principal = principals.iterator().next();

		if (super.login() == true) {
			return true;
		}

		// Put the principal name into the sharedState map
		sharedState.put("javax.security.auth.login.name", principal.getName()); //$NON-NLS-1$
		sharedState.put("javax.security.auth.login.password", ""); //$NON-NLS-1$ //$NON-NLS-2$
		super.loginOk = true;

		return true;
	}
	
	protected Principal getIdentity() {
		return principal;
	}

	protected Group[] getRoleSets() throws LoginException {
		return new Group[] {};
	}
}
