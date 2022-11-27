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
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.jboss.as.security.plugins.SecurityDomainContext;
import org.jboss.as.server.CurrentServiceContainer;
import org.jboss.msc.service.ServiceController;
import org.jboss.msc.service.ServiceName;
import org.jboss.security.AuthenticationManager;
import org.jboss.security.PicketBoxLogger;
import org.jboss.security.SecurityConstants;
import org.jboss.security.SecurityContext;
import org.jboss.security.SecurityRolesAssociation;
import org.jboss.security.SimplePrincipal;
import org.jboss.security.SubjectInfo;
import org.jboss.security.identity.RoleGroup;
import org.jboss.security.identity.plugins.SimpleRoleGroup;
import org.jboss.security.mapping.MappingContext;
import org.jboss.security.mapping.MappingManager;
import org.jboss.security.mapping.MappingType;
import org.jboss.security.negotiation.Constants;
import org.jboss.security.negotiation.common.NegotiationContext;
import org.jboss.security.negotiation.spnego.KerberosMessage;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.security.Credentials;
import org.teiid.security.GSSResult;
import org.teiid.security.SecurityHelper;

public class JBossSecurityHelper implements SecurityHelper, Serializable {
    private static final long serialVersionUID = 3598997061994110254L;
    public static final String AT = "@"; //$NON-NLS-1$

    @Override
    public SecurityContext associateSecurityContext(Object newContext) {
        SecurityContext context = SecurityActions.getSecurityContext();
        if (newContext != context) {
            SecurityActions.setSecurityContext((SecurityContext)newContext);
        }
        return context;
    }

    @Override
    public void clearSecurityContext() {
        SecurityActions.clearSecurityContext();
    }

    @Override
    public Object getSecurityContext(String securityDomain) {
        SecurityContext sc = SecurityActions.getSecurityContext();
        if (sc != null && sc.getSecurityDomain().equals(securityDomain)) {
            return sc;
        }
        return null;
    }

    public SecurityContext createSecurityContext(String securityDomain, Principal p, Object credentials, Subject subject) {
        return SecurityActions.createSecurityContext(p, credentials, subject, securityDomain);
    }

    @Override
    public Subject getSubjectInContext(Object context) {
        if (!(context instanceof SecurityContext)) {
            return null;
        }
        SecurityContext sc = (SecurityContext)context;
        SubjectInfo si = sc.getSubjectInfo();
        Subject subject = si.getAuthenticatedSubject();
        return subject;
    }

    @Override
    public SecurityContext authenticate(String domain,
            String baseUsername, Credentials credentials, String applicationName) throws LoginException {
        // If username specifies a domain (user@domain) only that domain is authenticated against.
        SecurityDomainContext securityDomainContext = getSecurityDomainContext(domain);
        if (securityDomainContext != null) {
            Subject subject = new Subject();
            boolean isValid = false;
            SecurityContext securityContext = null;
            AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();
            if (authManager != null) {
                Principal userPrincipal = new SimplePrincipal(baseUsername);
                Object cred = credentials==null?null:credentials.getCredentials();
                isValid = authManager.isValid(userPrincipal, cred, subject);
                securityContext = createSecurityContext(domain, userPrincipal, cred, subject);
                LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful for \"", baseUsername, "\" in security domain", domain}); //$NON-NLS-1$ //$NON-NLS-2$
            }

            if (isValid) {
                MappingManager mappingManager = securityDomainContext.getMappingManager();
                if (mappingManager != null) {
                    MappingContext<RoleGroup> mc = mappingManager.getMappingContext(MappingType.ROLE.name());
                    if(mc != null && mc.hasModules()) {
                        RoleGroup userRoles = securityContext.getUtil().getRoles();
                        if(userRoles == null) {
                            userRoles = new SimpleRoleGroup(SecurityConstants.ROLES_IDENTIFIER);
                         }

                        Map<String,Object> contextMap = new HashMap<String,Object>();
                        contextMap.put(SecurityConstants.ROLES_IDENTIFIER, userRoles);
                        //Append any deployment role->principals configuration done by the user
                        contextMap.put(SecurityConstants.DEPLOYMENT_PRINCIPAL_ROLES_MAP,
                              SecurityRolesAssociation.getSecurityRoles());

                        //Append the principals also
                        contextMap.put(SecurityConstants.PRINCIPALS_SET_IDENTIFIER, subject.getPrincipals());
                        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Roles before mapping \"", userRoles.toString()}); //$NON-NLS-1$
                        PicketBoxLogger.LOGGER.traceRolesBeforeMapping(userRoles != null ? userRoles.toString() : "");

                        mc.performMapping(contextMap, userRoles);
                        RoleGroup mappedRoles = mc.getMappingResult().getMappedObject();
                        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Roles after mapping \"", mappedRoles.toString()}); //$NON-NLS-1$
                    }
                }
                return securityContext;
            }
        }
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, baseUsername, domain ));
    }

    @Override
    public GSSResult negotiateGssLogin(String securityDomain, byte[] serviceTicket) throws LoginException {

        SecurityDomainContext securityDomainContext = getSecurityDomainContext(securityDomain);
        if (securityDomainContext != null) {
            AuthenticationManager authManager = securityDomainContext.getAuthenticationManager();

            if (authManager != null) {
                Object previous = null;
                NegotiationContext context = new NegotiationContext();
                context.setRequestMessage(new KerberosMessage(Constants.KERBEROS_V5, serviceTicket));

                try {
                    context.associate();
                    SecurityContext securityContext = createSecurityContext(securityDomain, new SimplePrincipal("temp"), null, new Subject()); //$NON-NLS-1$
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

                        Object sc = createSecurityContext(securityDomain, principal, null, subject);
                        LogManager.logDetail(LogConstants.CTX_SECURITY, new Object[] {"Logon successful though GSS API"}); //$NON-NLS-1$
                        GSSResult result = buildGSSResult(context, securityDomain, true, delegationCredential);
                        result.setSecurityContext(sc);
                        result.setUserName(principal.getName());
                        return result;
                    }
                    LoginException le = (LoginException)securityContext.getData().get("org.jboss.security.exception"); //$NON-NLS-1$
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
        throw new LoginException(IntegrationPlugin.Util.gs(IntegrationPlugin.Event.TEIID50072, "GSS Auth", securityDomain)); //$NON-NLS-1$
    }

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

    protected SecurityDomainContext getSecurityDomainContext(String securityDomain) {
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
