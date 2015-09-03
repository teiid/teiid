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
package org.teiid.jboss.oauth;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils;
import org.apache.cxf.rs.security.oauth2.common.ClientAccessToken;
import org.apache.cxf.rs.security.oauth2.grants.refresh.RefreshTokenGrant;
import org.teiid.OAuthCredential;

public class OAuth20CredentialImpl implements OAuthCredential {
    private String clientId;
    private String clientSecret;
    private String refreshToken;
    private String accessTokenURI;
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
        OAuthClientUtils.Consumer consumer = new OAuthClientUtils.Consumer(getClientId(), getClientSecret());
        WebClient client = WebClient.create(getAccessTokenURI());
        RefreshTokenGrant grant = new RefreshTokenGrant(getRefreshToken());
        return OAuthClientUtils.getAccessToken(client, consumer, grant, null, false);
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
}
