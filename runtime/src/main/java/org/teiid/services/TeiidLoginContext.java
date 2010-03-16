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

import java.io.IOException;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.teiid.runtime.RuntimePlugin;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;

import com.metamatrix.common.log.LogConstants;
import com.metamatrix.common.log.LogManager;

/**
 * This class serves as the primary implementation of the
 * Membership Service. Based on the security domains specified this class delegates the responsibility of
 * authenticating user to those security domains in the order they are defined.
 */
public class TeiidLoginContext {
	public static final String AT = "@"; //$NON-NLS-1$
	private LoginContext loginContext;
	private String userName;
	private String securitydomain;
	private Object credentials;
	
	public void authenticateUser(String username, Credentials credential, String applicationName, List<String> domains) throws LoginException {
        
        LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, new Object[] {"authenticateUser", username, applicationName}); //$NON-NLS-1$
                
        final String baseUsername = getBaseUsername(username);
        final char[] password = credential.getCredentialsAsCharArray();
           
        // If username specifies a domain (user@domain) only that domain is authenticated against.
        // If username specifies no domain, then all domains are tried in order.
        for (String domain:getDomainsForUser(domains, username)) {
        	
            try {
        		CallbackHandler handler = new CallbackHandler() {
					@Override
					public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
						for (int i = 0; i < callbacks.length; i++) {
							if (callbacks[i] instanceof NameCallback) {
								NameCallback nc = (NameCallback)callbacks[i];
								nc.setName(baseUsername);
							} else if (callbacks[i] instanceof PasswordCallback) {
								PasswordCallback pc = (PasswordCallback)callbacks[i];
								pc.setPassword(password);
								credentials = password;
							} else {
								throw new UnsupportedCallbackException(callbacks[i], "Unrecognized Callback"); //$NON-NLS-1$
							}
						}
					}
        		};      	
            	
        		// this is the configured login for teiid
        		this.loginContext = createLoginContext(domain,handler);
				this.loginContext.login();
				this.userName = baseUsername+AT+domain;
				this.securitydomain = domain;
				LogManager.logDetail(LogConstants.CTX_MEMBERSHIP, new Object[] {"Logon successful for \"", username, "\""}); //$NON-NLS-1$ //$NON-NLS-2$
				return;
			} catch (LoginException e) {
				LogManager.logDetail(LogConstants.CTX_MEMBERSHIP,e, e.getMessage()); 
			}
        }
        throw new LoginException(RuntimePlugin.Util.getString("SessionServiceImpl.The_username_0_and/or_password_are_incorrect", username )); //$NON-NLS-1$       
    }
    
    protected LoginContext createLoginContext(String domain, CallbackHandler handler) throws LoginException {
    	return new LoginContext(domain, handler);
    }
    
    public LoginContext getLoginContext() {
    	return this.loginContext;
    }
    
    public String getUserName() {
    	return this.userName;
    }
    
    public String getSecurityDomain() {
    	return this.securitydomain;
    }
    
    public Object getSecurityContext(SecurityHelper helper) {
    	Object sc = null;
        if (this.loginContext != null) {
        	sc = helper.getSecurityContext(this.securitydomain);
        	if ( sc == null){
	        	Subject subject = this.loginContext.getSubject();
	        	Principal principal = null;
	        	for(Principal p:subject.getPrincipals()) {
	        		if (this.userName.startsWith(p.getName())) {
	        			principal = p;
	        			break;
	        		}
	        	}
	        	return helper.createSecurityContext(this.securitydomain, principal, credentials, subject);
        	}
        }
    	return sc;
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
