/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.jboss;

import java.io.Serializable;
import java.security.AccessController;
import java.security.Principal;
import java.util.Collections;
//import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

//import org.ietf.jgss.GSSContext;
//import org.ietf.jgss.GSSCredential;
//import org.ietf.jgss.GSSException;
//import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.server.CurrentServiceContainer;
import org.jboss.msc.service.ServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
//import org.jboss.security.AuthenticationManager;
//import org.jboss.security.SimplePrincipal;
//import org.jboss.security.negotiation.Constants;
//import org.jboss.security.negotiation.common.NegotiationContext;
//import org.jboss.security.negotiation.spnego.KerberosMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

public class JBossSecurityHelper implements SecurityHelper<SecurityIdentity>, Serializable {
    private static final long serialVersionUID = 3598997061994110254L;
    public static final String AT = "@"; //$NON-NLS-1$

    public static final ServiceName WILDFLY = ServiceName.of("org","wildfly");

    @Override
    public SecurityIdentity associateSecurityContext(SecurityIdentity newContext) {
        SecurityIdentity context = SecurityActions.getSecurityIdentity();
        if (newContext != context) {
            SecurityActions.setSecurityIdentity(newContext);
        }
        return context;
    }

    @Override
    public void clearSecurityContext() {
        SecurityActions.clearSecurityIdentity();
    }

    @Override
    public SecurityIdentity getSecurityContext(String securityDomain) {
        SecurityIdentity securityIdentity = SecurityActions.getSecurityIdentity();
        if (securityIdentity != null/* && sc.getSecurityDomain().equals(securityDomain)*/) { //todo how to compare domains
            return securityIdentity;
        }
        return null;
    }

    public SecurityIdentity createSecurityIdentity(String securityDomain, Principal p, Object credentials) {
        return SecurityActions.createSecurityIdentity(p, credentials, securityDomain ,this);
    }

    @Override
    public Subject getSubjectInContext(SecurityIdentity securityIdentity) {
        Subject subject = null;

        if(securityIdentity != null){
            subject = new Subject(true, Collections.singleton(securityIdentity.getPrincipal()),Collections.singleton(securityIdentity.getPublicCredentials()), Collections.singleton(securityIdentity.getPrivateCredentials()));
         }
        return subject;
    }

    @Override
    public SecurityIdentity authenticate(String domain,
            String baseUsername, Credentials credentials, String applicationName) throws LoginException {

        SecurityDomain securityDomain = getSecurityDomain(domain);
        if (securityDomain != null) {
            try {
                SecurityIdentity securityIdentity = securityDomain.authenticate(baseUsername, new PasswordGuessEvidence(credentials.getCredentials().toString().toCharArray()));
                if (securityIdentity != null) {
                    LogManager.logDetail(LogConstants.CTX_SECURITY, "Authenticated successful for '"+baseUsername+"'", securityIdentity.getRoles().toString());
                    return securityIdentity;
                } else {
                    LogManager.logInfo(LogConstants.CTX_SECURITY, "user \""+baseUsername+"\" was not authenticated"); //$NON-NLS-1$ //$NON-NLS-2$
                    throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, baseUsername, domain));
                }
            } catch (RealmUnavailableException e) {
                throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, baseUsername, domain));
            }
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, baseUsername, domain));
    }

    @Override
    public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {

/*
        SecurityDomainContext securityDomainContext = getSecurityDomainContext(securityDomain);
        if (securityDomainContext != null) {
            AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();

            if (authManager != null) {
                SecurityIdentity previous = null;
                NegotiationContext context = new NegotiationContext();
                context.setRequestMessage(new KerberosMessage(Constants.KERBEROS_V5, serviceTicket));

                try {
                    context.associate();
                    SecurityIdentity securityContext = createSecurityIdentity(securityDomain, new SimplePrincipal("temp"), null); //$NON-NLS-1$
                    previous = associateSecurityContext(securityContext);

                    Subject subject = new Subject();
                    boolean isValid = authManager.isValid(null, null, subject);
                    if (isValid) {

                        Principal principal = null;
                        for(Principal p:subject.getPrincipals()) {
                            principal = p;
                            break;
                        }
                        GSSCredential delegationCredential = null;
                        //if isValid checked just the cache the context will be null
                        if (context.getSchemeContext() == null) {
                            Set<GSSCredential> credentials = subject.getPrivateCredentials(GSSCredential.class);
                            if (credentials != null && !credentials.isEmpty()) {
                                delegationCredential = credentials.iterator().next();
                            }
                        }

                        Object sc = createSecurityIdentity(securityDomain, principal, null);
                        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful though GSS API"}); //$NON-NLS-1$
                        GSSResult result = buildGSSResult(context, securityDomain, true, delegationCredential);
                        result.setSecurityContext(sc);
                        result.setUserName(principal.getName());
                        return result;
                    }
                    LoginException le = null;//(LoginException)securityContext.getData().get("org.jboss.security.exception"); //$NON-NLS-1$
                    if (le != null) {
                        if (le.getMessage().equals("Continuation Required.")) { //$NON-NLS-1$
                            return buildGSSResult(context, securityDomain, false, null);
                        }
                        throw le;
                    }
                } finally {
                    associateSecurityContext(previous);
                    context.clear();
                }
            }
        }
*/
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, "GSS Auth", securityDomain)); //$NON-NLS-1$
    }

/*
    private GSSResult buildGSSResult(NegotiationContext context, String securityDomain, boolean validAuth, GSSCredential delegationCredential) throws LoginException {
        GSSContext securityContext = (GSSContext) context.getSchemeContext();
        try {
            if (securityContext != null && securityContext.getCredDelegState()) {
                delegationCredential = securityContext.getDelegCred();
            }
            if (context.getResponseMessage() == null && validAuth) {
                return new GSSResult(context.isAuthenticated(), delegationCredential);
            }
            if (context.getResponseMessage() instanceof KerberosMessage) {
                KerberosMessage km = (KerberosMessage)context.getResponseMessage();
                return new GSSResult(km.getToken(), context.isAuthenticated(), delegationCredential);
            }
        } catch (GSSException e) {
            // login exception can not take exception
            throw new LoginException(e.getMessage());
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50103, securityDomain));
    }
*/

/*
    protected SecurityDomainContext getSecurityDomainContext(String securityDomain) {
        if (securityDomain != null && !securityDomain.isEmpty()) {
            ServiceName name = ServiceName.JBOSS.append("security", "security-domain", securityDomain); //$NON-NLS-1$ //$NON-NLS-2$
            ServiceController<SecurityDomainContext> controller = (ServiceController<SecurityDomainContext>)currentServiceContainer().getService(name);
            if (controller != null) {
                return controller.getService().getValue();
            }
        }
        return null;
    }
*/

    public SecurityDomain getSecurityDomain(String securityDomainName) {
        SecurityDomain securityDomain = null; //SecurityDomain.getCurrent(); // this only registers one
        if (securityDomain == null) {
            ServiceName serviceName = WILDFLY.append("security", "security-domain", securityDomainName);
            ServiceController<?> controller = currentServiceContainer().getService(serviceName);
            if (controller != null) {
                securityDomain = (SecurityDomain) controller.getValue();
            }
        }
        return securityDomain;
    }

    private ServiceContainer currentServiceContainer() {
        return System.getSecurityManager() == null ? CurrentServiceContainer.getServiceContainer() : AccessController.doPrivileged(CurrentServiceContainer.GET_ACTION);
    }
}
