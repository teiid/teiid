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

package org.teiid.google.auth;


public class OAuth2HeaderFactory implements AuthHeaderFactory {
    private OAuth2Tokens tokens = null;
    private OAuth2Authenticator authenticator = new OAuth2Authenticator();
    private String clientId = OAuth2Authenticator.CLIENT_ID;
    private String clientSecret = OAuth2Authenticator.CLIENT_SECRET;

    public OAuth2HeaderFactory(String refreshToken){
        this.tokens = new OAuth2Tokens("", refreshToken, 1000); //$NON-NLS-1$
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setClientSecret(String clientSecret) {
        this.clientSecret = clientSecret;
    }

    public void refreshToken() {
        tokens = authenticator.refreshToken(tokens, clientId, clientSecret);
    }

    @Override
    public String getAuthHeader() {
        return "Bearer "+  tokens.getAccessToken(); //$NON-NLS-1$
    }

}
