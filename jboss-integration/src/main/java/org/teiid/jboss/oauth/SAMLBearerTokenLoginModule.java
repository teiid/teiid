/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2006, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.teiid.jboss.oauth;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils;
import org.apache.cxf.rs.security.oauth2.common.ClientAccessToken;
import org.apache.cxf.rs.security.oauth2.grants.saml.Saml2BearerGrant;

public abstract class SAMLBearerTokenLoginModule extends OAuth20LoginModule {
    private String scope;
    
    @Override
    public void initialize(Subject subject, CallbackHandler handler, Map<String, ?> sharedState, Map<String, ?> options) {
       super.initialize(subject, handler, sharedState, options);
       
       this.scope = (String) options.get("scope"); //$NON-NLS-1$
    }
    
    @Override
    public boolean login() throws LoginException {
        this.callerSubject = getSubject();
        this.callerPrincipal = getPrincipal();
        
        final String samlToken = getSAMLResponseToken();
        if (samlToken == null) {
            return false;
        }
        
        OAuth20CredentialImpl cred = new OAuth20CredentialImpl() {
            protected ClientAccessToken getAccessToken() {
                OAuthClientUtils.Consumer consumer = new OAuthClientUtils.Consumer(getClientId(), getClientSecret());
                WebClient client = WebClient.create(getAccessTokenURI());
                Saml2BearerGrant grant = null;
                if (scope != null) {
                    grant = new Saml2BearerGrant(samlToken, scope);
                }
                else {
                    grant = new Saml2BearerGrant(samlToken);
                }
                return OAuthClientUtils.getAccessToken(client, consumer, grant, null, false);
            }            
        };
        cred.setClientId(getClientId());
        cred.setClientSecret(getClientSecret());
        cred.setAccessTokenURI(getAccessTokenURI());
        setCredential(cred);
        return super.login();
    }

    public abstract String getSAMLResponseToken();
}
