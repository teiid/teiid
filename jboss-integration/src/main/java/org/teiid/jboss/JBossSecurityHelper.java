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

import java.io.Serializable;
import java.security.Principal;

import javax.security.auth.Subject;

import org.jboss.security.SecurityContext;
import org.teiid.SecurityHelper;

public class JBossSecurityHelper implements SecurityHelper, Serializable {

	@Override
	public boolean assosiateSecurityContext(String securityDomain, Object newContext) {
		SecurityContext context = SecurityActions.getSecurityContext();
		if (context == null || (!context.getSecurityDomain().equals(securityDomain) && newContext != null)) {
			SecurityActions.setSecurityContext((SecurityContext)newContext);
			return true;
		}
		return false;
	}

	@Override
	public void clearSecurityContext(String securityDomain) {
		SecurityContext sc = SecurityActions.getSecurityContext();
		if (sc != null && sc.getSecurityDomain().equals(securityDomain)) {
			SecurityActions.clearSecurityContext();
		}
	}
	
	@Override
	public Object getSecurityContext(String securityDomain) {
		SecurityContext sc = SecurityActions.getSecurityContext();
		if (sc != null && sc.getSecurityDomain().equals(securityDomain)) {
			return sc;
		}
		return null;
	}	
	
	@Override
	public Object createSecurityContext(String securityDomain, Principal p, Object credentials, Subject subject) {
		SecurityActions.pushSecurityContext(p, credentials, subject, securityDomain);
		return getSecurityContext(securityDomain);
	}
	
}
