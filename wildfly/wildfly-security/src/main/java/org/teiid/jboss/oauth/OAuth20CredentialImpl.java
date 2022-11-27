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

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.rs.security.oauth2.client.Consumer;
import org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils;
import org.apache.cxf.rs.security.oauth2.common.ClientAccessToken;
import org.apache.cxf.rs.security.oauth2.grants.refresh.RefreshTokenGrant;
import org.apache.cxf.rs.security.oauth2.utils.OAuthConstants;
import org.teiid.OAuthCredential;

public class OAuth20CredentialImpl implements OAuthCredential {
    private String clientId;
    private String clientSecret;
    private String refreshToken;
    private String accessTokenURI;
    private String accessTokenString;
    private ClientAccessToken accessToken;

    @Override
    public String getAuthorizationHeader(String resourceURI, String httpMethod) {
        if (this.accessToken == null || expired(this.accessToken)) {
            this.accessToken = getAccessToken();
        }
        return "Bearer "+this.accessToken.getTokenKey(); //$NON-NLS-1$
    }

    @Override
    public String getAuthrorizationProperty(String key) {
        if (this.accessToken == null || expired(this.accessToken)) {
            this.accessToken = getAccessToken();
        }
        return this.accessToken.getParameters().get(key);
    }

    private boolean expired(ClientAccessToken token) {
        if (token.getExpiresIn() != -1) {
            return (((token.getIssuedAt()+token.getExpiresIn())-System.currentTimeMillis()) < 0);
        }
        return false;
    }

    protected ClientAccessToken getAccessToken() {
        if (getAccessTokenString() != null) { // if we have access_token directly, use it
            return new ClientAccessToken(OAuthConstants.ACCESS_TOKEN_TYPE, getAccessTokenString());
        }
        Consumer consumer = new Consumer(getClientId(), getClientSecret());
        WebClient client = WebClient.create(getAccessTokenURI());
        RefreshTokenGrant grant = new RefreshTokenGrant(getRefreshToken());
        return OAuthClientUtils.getAccessToken(client, consumer, grant, null, "Bearer", false);
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public String getClientSecret() {
        return clientSecret;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public void setRefreshToken(String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getAccessTokenURI() {
        return accessTokenURI;
    }

    public void setAccessTokenURI(String accessTokenURI) {
        this.accessTokenURI = accessTokenURI;
    }

    public String getAccessTokenString() {
        return accessTokenString;
    }

    public void setAccessTokenString(String accessTokenString) {
        this.accessTokenString = accessTokenString;
    }
}
