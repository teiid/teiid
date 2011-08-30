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

package org.teiid.services;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.SimplePrincipal;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;


/**
 * This class serves as the primary implementation of the
 * Membership Service. Based on the security domains specified this class delegates the responsibility of
 * authenticating user to those security domains in the order they are defined.
 */
public class TeiidLoginContext {
	public static final String AT = "@"; //$NON-NLS-1$
	private Subject subject;
	private String userName;
	private String securitydomain;
	private Object securityContext;
	private SecurityHelper securityHelper;
	
	public TeiidLoginContext(SecurityHelper helper) {
		this.securityHelper = helper;
	}
	
	public void authenticateUser(String username, final Credentials credential, 
			String applicationName, List<String> domains, Map<String, SecurityDomainContext> securityDomainMap, boolean onlyallowPassthrough) 
		throws LoginException {
        
        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"authenticateUser", username, applicationName}); //$NON-NLS-1$
                
        final String baseUsername = getBaseUsername(username);

    	if (onlyallowPassthrough) {
            for (String domain:getDomainsForUser(domains, username)) {
	        	Subject existing = this.securityHelper.getSubjectInContext(domain);
	        	if (existing != null) {
					this.userName = getUserName(existing)+AT+domain;
					this.securitydomain = domain;     
					this.subject = existing;
					this.securityContext = this.securityHelper.getSecurityContext(domain);
					return;
	        	}
            }
            throw new LoginException(RuntimePlugin.Util.getString("no_passthrough_identity_found")); //$NON-NLS-1$
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
                    boolean isValid = authManager.isValid(userPrincipal, new String(credential.getCredentialsAsCharArray()), subject);
                    if (isValid) {
        				this.userName = baseUsername+AT+domain;
        				this.securitydomain = domain;
        				this.securityContext = this.securityHelper.createSecurityContext(this.securitydomain, userPrincipal, new String(credential.getCredentialsAsCharArray()), subject);
        				LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful for \"", username, "\""}); //$NON-NLS-1$ //$NON-NLS-2$
        				return;
                    }            			
        		}
        	}
        }
        throw new LoginException(RuntimePlugin.Util.getString("SessionServiceImpl.The_username_0_and/or_password_are_incorrect", username )); //$NON-NLS-1$       
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
    
    public String getUserName() {
    	return this.userName;
    }
    
    public String getSecurityDomain() {
    	return this.securitydomain;
    }
    
    public Subject getSubject() {
    	return this.subject;
    }
    
    public Object getSecurityContext() {
    	return this.securityContext;
    }
    
	public LoginContext createLoginContext(String domain, CallbackHandler handler) throws LoginException {
    	return new LoginContext(domain, handler);
    }    
    
    static String getBaseUsername(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);

        String result = username;
        
        if (index != -1) {
            result = username.substring(0, index);
        }
        
        //strip the escape character from the remaining ats
        return result.replaceAll("\\\\"+AT, AT); //$NON-NLS-1$
    }
    
    static String escapeName(String name) {
        if (name == null) {
            return name;
        }
        
        return name.replaceAll(AT, "\\\\"+AT); //$NON-NLS-1$
    }
    
    static String getDomainName(String username) {
        if (username == null) {
            return username;
        }
        
        int index = getQualifierIndex(username);
        
        if (index != -1) {
            return username.substring(index + 1);
        }
        
        return null;
    }
    
    static int getQualifierIndex(String username) {
        int index = username.length();
        while ((index = username.lastIndexOf(AT, --index)) != -1) {
            if (index > 0 && username.charAt(index - 1) != '\\') {
                return index;
            }
        }
        
        return -1;
    }
    
    private Collection<String> getDomainsForUser(List<String> domains, String username) {
    	// If username is null, return all domains
        if (username == null) {
            return domains;
        }  
        
        String domain = getDomainName(username);
        
        if (domain == null) {
        	return domains;
        }
        
        // ------------------------------------------
        // Handle usernames having @ sign
        // ------------------------------------------
        String domainHolder = null;
        for (String d:domains) {
        	if(d.equalsIgnoreCase(domain)) {
        		domainHolder = d;
        		break;
        	}        	
        }
        
        if (domainHolder == null) {
            return Collections.emptyList();
        }
        
        LinkedList<String> result = new LinkedList<String>();
        result.add(domainHolder);
        return result;
    }       
}
