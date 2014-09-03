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

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.server.CurrentServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.SecurityContext;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.negotiation.Constants;
import org.jboss.security.negotiation.common.NegotiationContext;
import org.jboss.security.negotiation.spnego.KerberosMessage;
import org.teiid.dqp.service.GSSResult;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.security.Credentials;
import org.teiid.services.SessionServiceImpl;
import org.teiid.services.TeiidLoginContext;

public class JBossSessionService extends SessionServiceImpl {
	
	@Override
	protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, String domain)
			throws LoginException {
        final String baseUsername = getBaseUsername(userName);

        // If username specifies a domain (user@domain) only that domain is authenticated against.
        // If username specifies no domain, then all domains are tried in order.
    		// this is the configured login for teiid
    	SecurityDomainContext securityDomainContext = getSecurityDomain(domain);
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
	
	@Override
	public GSSResult neogitiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {
		
		SecurityDomainContext securityDomainContext = getSecurityDomain(securityDomain);
		if (securityDomainContext != null) {
			AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();

			if (authManager != null) {
    			Object previous = null;    				
				NegotiationContext context = new NegotiationContext();
				context.setRequestMessage(new KerberosMessage(Constants.KERBEROS_V5, serviceTicket));
				
				try {
					context.associate();
    				SecurityContext securityContext = (SecurityContext)this.securityHelper.createSecurityContext(securityDomain, new SimplePrincipal("temp"), null, new Subject()); //$NON-NLS-1$
    				previous = this.securityHelper.associateSecurityContext(securityContext);
					
    				Subject subject = new Subject();
	                boolean isValid = authManager.isValid(null, null, subject);
	                if (isValid) {

	                	Principal principal = null;
	    		    	for(Principal p:subject.getPrincipals()) {
	    					principal = p;
	    					break;
	    		    	}
	                	
	                	Object sc = this.securityHelper.createSecurityContext(securityDomain, principal, null, subject);
	    				LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful though GSS API"}); //$NON-NLS-1$
	    				GSSResult result = buildGSSResult(context, securityDomain);
	    				result.setSecurityContext(sc);
	    				result.setUserName(principal.getName());
	    				return result;
	                }
                	LoginException le = (LoginException)securityContext.getData().get("org.jboss.security.exception"); //$NON-NLS-1$
                	if (le != null) {
                		if (le.getMessage().equals("Continuation Required.")) { //$NON-NLS-1$
                			return buildGSSResult(context, securityDomain);
                		}
               			throw le;
                	}
				} finally {
					this.securityHelper.associateSecurityContext(previous);
					context.clear();
				}
    		}
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, "GSS Auth" )); //$NON-NLS-1$		
	}

	private GSSResult buildGSSResult(NegotiationContext context, String securityDomain) throws LoginException {
		if (context.getResponseMessage() instanceof KerberosMessage) {
			try {
                KerberosMessage km = (KerberosMessage)context.getResponseMessage();
                GSSContext securityContext = (GSSContext) context.getSchemeContext();			
                return new GSSResult(km.getToken(), context.isAuthenticated(), securityContext.getCredDelegState()?securityContext.getDelegCred():null);
            } catch (GSSException e) {
                // login exception can not take exception
                throw new LoginException(e.getMessage());
            }
		}
		throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50103, securityDomain));
	} 	
	
	public SecurityDomainContext getSecurityDomain(String securityDomain) {
		if (securityDomain != null && !securityDomain.isEmpty()) {
			ServiceName name = ServiceName.JBOSS.append("security", "security-domain", securityDomain); //$NON-NLS-1$ //$NON-NLS-2$
			ServiceController<SecurityDomainContext> controller = (ServiceController<SecurityDomainContext>) CurrentServiceContainer.getServiceContainer().getService(name);
			if (controller != null) {
				return controller.getService().getValue();
			}
		}
		return null;
	}
}
