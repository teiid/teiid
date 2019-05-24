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