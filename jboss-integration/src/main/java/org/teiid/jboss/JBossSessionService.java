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
import java.security.acl.Group;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, List<String> domains, boolean onlyallowPassthrough)
			throws LoginException {
        return authenticateUser(userName, credentials, applicationName, domains, securityDomainMap, onlyallowPassthrough);                        
	}
	
	private TeiidLoginContext authenticateUser(String username, final Credentials credential, String applicationName, List<String> domains, Map<String, SecurityDomainContext> securityDomainMap, boolean onlyallowPassthrough) 
		throws LoginException {
		
        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"authenticateUser", username, applicationName}); //$NON-NLS-1$
                
        final String baseUsername = getBaseUsername(username);

    	if (onlyallowPassthrough) {
            for (String domain:getDomainsForUser(domains, username)) {
	        	Subject existing = this.securityHelper.getSubjectInContext(domain);
	        	if (existing != null) {
					return new TeiidLoginContext(getUserName(existing)+AT+domain, existing, domain, this.securityHelper.getSecurityContext(domain));
	        	}
            }
            throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50073));
    	}

        
        // If username specifies a domain (user@domain) only that domain is authenticated against.
        // If username specifies no domain, then all domains are tried in order.
        for (String domain:getDomainsForUser(domains, username)) {
    		// this is the configured login for teiid
        	SecurityDomainContext securityDomainContext = securityDomainMap.get(domain);
        	if (securityDomainContext != null) {
        		AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();
        		if (authManager != null) {
                    Principal userPrincipal = new SimplePrincipal(username);
                    Subject subject = new Subject();
                    boolean isValid = authManager.isValid(userPrincipal, credential==null?null:new String(credential.getCredentialsAsCharArray()), subject);
                    if (isValid) {
        				String userName = baseUsername+AT+domain;
        				Object securityContext = this.securityHelper.createSecurityContext(domain, userPrincipal, credential==null?null:new String(credential.getCredentialsAsCharArray()), subject);
        				LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful for \"", username, "\""}); //$NON-NLS-1$ //$NON-NLS-2$
        				return new TeiidLoginContext(userName, subject, domain, securityContext);
                    }            			
        		}
        	}
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, username ));       
    }	
	
	private String getUserName(Subject subject) {
		Set<Principal> principals = subject.getPrincipals();
		for (Principal p:principals) {
			if (p instanceof Group) {
				continue;
			}
			return p.getName();
		}
		return null;
	}
	
}
