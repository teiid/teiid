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

import java.io.Serializable;

import org.apache.cxf.rs.security.oauth.client.OAuthClientUtils;
import org.teiid.OAuthCredential;

/**
 * This helps aid with OAuth1.0a authentication. WS resource-adapter will look for instance of this class in current 
 * subject's credentials. 
 */
public class OAuth10CredentialImpl implements OAuthCredential, Serializable {
    private static final long serialVersionUID = 7478594712832822465L;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessSecret;
    
    public String getAuthorizationHeader(String resourceURI, String httpMethod) {
        OAuthClientUtils.Consumer consumer = new OAuthClientUtils.Consumer(this.consumerKey, this.consumerSecret);
        OAuthClientUtils.Token accessToken = new OAuthClientUtils.Token(this.accessToken, this.accessSecret);
        return OAuthClientUtils.createAuthorizationHeader(consumer, accessToken, httpMethod, resourceURI);        
    }
    
    @Override
    public String getAuthrorizationProperty(String key) {
        // for now only in OAUTH2
        return null;
    }    

    public String getConsumerKey() {
        return consumerKey;
    }

    public void setConsumerKey(String consumerKey) {
        this.consumerKey = consumerKey;
    }

    public String getConsumerSecret() {
        return consumerSecret;
    }

    public void setConsumerSecret(String consumerSecret) {
        this.consumerSecret = consumerSecret;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public void setAccessSecret(String accessSecret) {
        this.accessSecret = accessSecret;
    }   
}