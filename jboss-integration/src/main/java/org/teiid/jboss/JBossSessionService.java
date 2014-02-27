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

import java.security.Principal;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.SimplePrincipal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.security.Credentials;
import org.teiid.services.SessionServiceImpl;
import org.teiid.services.TeiidLoginContext;

public class JBossSessionService extends SessionServiceImpl {

	private Map<String, SecurityDomainContext> securityDomainMap;
	
	public JBossSessionService(Map<String, SecurityDomainContext> securityDomainMap) {
		this.securityDomainMap = securityDomainMap;
	}
		
	@Override
	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, String domain)
			throws LoginException {
        final String baseUsername = getBaseUsername(userName);

        // If username specifies a domain (user@domain) only that domain is authenticated against.
        // If username specifies no domain, then all domains are tried in order.
    		// this is the configured login for teiid
    	SecurityDomainContext securityDomainContext = securityDomainMap.get(domain);
    	if (securityDomainContext != null) {
    		AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();
    		if (authManager != null) {
                Principal userPrincipal = new SimplePrincipal(userName);
                Subject subject = new Subject();
                String credString = credentials==null?null:new String(credentials.getCredentialsAsCharArray());
                boolean isValid = authManager.isValid(userPrincipal, credString, subject);
                if (isValid) {
    				String qualifiedUserName = baseUsername+AT+domain;
    				Object securityContext = this.securityHelper.createSecurityContext(domain, userPrincipal, credString, subject);
    				LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful for \"", userName, "\""}); //$NON-NLS-1$ //$NON-NLS-2$
    				return new TeiidLoginContext(qualifiedUserName, subject, domain, securityContext);
                }            			
    		}
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, userName ));       
    }			
}
