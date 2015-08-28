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

package org.teiid.security;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

public interface SecurityHelper {

	/**
	 * Associate the given context and return the old context
	 * @param context
	 * @return
	 */
	Object associateSecurityContext(Object context);
	
	/**
	 * Clear any security context associated with the thread 
	 */
	void clearSecurityContext();
	
	/**
	 * Get the current security context associated with the thread
	 * @return
	 */
	Object getSecurityContext();
	
	/**
	 * Get the subject associated with the security context.
	 * The security context must currently be associated with the thread.
	 * @param securityDomain
	 * @return
	 */
	Subject getSubjectInContext(String securityDomain);
	
	/**
	 * Get the subject associated with the security context.
	 * @param context
	 * @return
	 */
	Subject getSubjectInContext(Object context);
	
	/**
	 * Authenticate the user and return the security context
	 * @param securityDomain
	 * @param baseUserName without the security domain suffix
	 * @param credentials
	 * @param applicationName
	 * @return
	 * @throws LoginException
	 */
	Object authenticate(String securityDomain, String baseUserName, Credentials credentials, String applicationName)
            throws LoginException;
    
	/**
	 * Negotiate the GSS login
	 * @param securityDomain
	 * @param serviceTicket
	 * @return
	 * @throws LoginException
	 */
	GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException;
	
}
