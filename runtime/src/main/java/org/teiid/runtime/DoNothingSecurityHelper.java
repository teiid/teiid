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

package org.teiid.runtime;

import java.security.Principal;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.teiid.security.TeiidLoginContext;

/**
 * A {@link SecurityHelper} that does nothing and always assumes that thread
 * has the proper security context.
 */
public class DoNothingSecurityHelper implements SecurityHelper {
	@Override
	public boolean sameSubject(String securityDomain, Object context,
			Subject subject) {
		return true;
	}

	@Override
	public Subject getSubjectInContext(String securityDomain) {
		return new Subject();
	}

	@Override
	public Object getSecurityContext() {
		return new Object();
	}

	@Override
	public Object createSecurityContext(String securityDomain, Principal p,
			Object credentials, Subject subject) {
		return new Object();
	}

	@Override
	public void clearSecurityContext() {

	}

	@Override
	public Object associateSecurityContext(Object context) {
		return null;
	}

    @Override
    public TeiidLoginContext authenticate(String securityDomain, String userName, Credentials credentials, String applicationName)
            throws LoginException {
        return new TeiidLoginContext(userName+"@"+securityDomain, new Subject(), securityDomain, new Object()); //$NON-NLS-1$
    }

    @Override
    public GSSResult neogitiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
        return null;
    }

    @Override
    public TeiidLoginContext passThroughLogin(String securityDomain, String userName) throws LoginException {
        Subject existing = getSubjectInContext(securityDomain);
        if(existing != null) {
            return new TeiidLoginContext(userName+"@"+securityDomain, existing, securityDomain, //$NON-NLS-1$
                    getSecurityContext());
        }
        throw new LoginException(RuntimePlugin.Util.gs(RuntimePlugin.Event.TEIID40087));
    }
}