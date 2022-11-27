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
package org.teiid.jboss.oauth;

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.rs.security.oauth2.client.Consumer;
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
                Consumer consumer = new Consumer(getClientId(), getClientSecret());
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
