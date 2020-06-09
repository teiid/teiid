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

package org.teiid.google.v4;

import static org.teiid.google.v4.ClientConstants.*;

import java.io.IOException;

import org.teiid.google.auth.AuthHeaderFactory;
import org.teiid.translator.google.api.SpreadsheetAuthException;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;

public class OAuth2HeaderFactory implements AuthHeaderFactory {
    private GoogleCredential credential = null;
    private String refreshToken;
    private String clientId;
    private String clientSecret;

    public OAuth2HeaderFactory(String refreshToken, String clientId, String clientSecret){
        this.refreshToken = refreshToken;
        this.clientId = clientId;
        this.clientSecret = clientSecret;
    }

    public void refreshToken() {
        try {
            if (credential == null) {
                credential = new GoogleCredential.Builder()
                    .setClientSecrets(clientId, clientSecret)
                    .setTransport(HTTP_TRANSPORT)
                    .setJsonFactory(JSON_FACTORY)
                    .build();
                credential.setRefreshToken(refreshToken);
            }
            credential.refreshToken();
        } catch (IOException e) {
            throw new SpreadsheetAuthException(
                    "Error reading TokenRequest response", e);
        }
    }

    public String getAuthHeader() {
        return "Bearer "+  credential.getAccessToken(); //$NON-NLS-1$
    }

    public GoogleCredential getCredential() {
        return credential;
    }

}
