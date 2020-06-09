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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.teiid.google.dataprotocol.GoogleDataProtocolAPI;
import org.teiid.google.dataprotocol.GoogleJSONParser;
import org.teiid.translator.google.api.SpreadsheetAuthException;


/**
 * This authenticator is used to work with OAuth2 for devices[1]. This class can be used for
 * two purposes:
 *  - Allowing user to give permission to Teiid Connector thus creating a refresh and access tokens
 *  - Allowing refreshing the access token using the refresh token.
 *
 *
 * The authResponse contains link which user has to type into the browser. Then he has to enter
 * userCode.
 *
 * After he has done that we can use the deviceCode to retrieve access and refresh tokens:
 * <code>
 * System.out.println(oauth.getAccessGoogleTokens(authResponse.getDeviceCode()));
 * </code>
 *
 * [1] https://developers.google.com/accounts/docs/OAuth2ForDevices
 * @author fnguyen
 *
 */
public class OAuth2Authenticator {
    final static String CLIENT_ID = "217138521084.apps.googleusercontent.com"; //$NON-NLS-1$
    final static String CLIENT_SECRET = "gXQ6-lOkEjE1lVcz7giB4Poy"; //$NON-NLS-1$
//    private final String SCOPE_DRIVE = "https://docs.google.com/feeds";
    private final String TOKEN_URL = "https://accounts.google.com/o/oauth2/token"; //$NON-NLS-1$


    /**
     *
     * @param clientSecret
     * @param clientId
     * @param at immutable instance of OAuth2Tokens
     * @return
     */
//    curl --data-urlencode client_id=217138521084.apps.googleusercontent.com \
//    --data-urlencode client_secret=gXQ6-lOkEjE1lVcz7giB4Poy \
//    --data-urlencode refresh_token=1/A6ifXgNxCYVGTkPUTnD6Y35v_GyfmuRAsKKL4eww8xs \
//    --data-urlencode grant_type=refresh_token \
//    https://accounts.google.com/o/oauth2/token
    public OAuth2Tokens refreshToken(OAuth2Tokens at, String clientId, String clientSecret) {

        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        nvps.add(new BasicNameValuePair("client_id", clientId)); //$NON-NLS-1$
        nvps.add(new BasicNameValuePair("client_secret", clientSecret)); //$NON-NLS-1$
        nvps.add(new BasicNameValuePair("refresh_token", at.getRefreshToken())); //$NON-NLS-1$
        nvps.add(new BasicNameValuePair("grant_type", "refresh_token")); //$NON-NLS-1$ //$NON-NLS-2$
        Map<?, ?> json = jsonResponseHttpPost(TOKEN_URL, nvps);
        return new OAuth2Tokens(json.get("access_token"), //$NON-NLS-1$
                at.getRefreshToken(), json.get("expires_in")); //$NON-NLS-1$
    }

    private Map<?, ?> jsonResponseHttpPost(String url, List<NameValuePair> data) {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        HttpPost httpPost = new HttpPost(url);

        HttpResponse response = null;
        try {
            httpPost.setEntity(new UrlEncodedFormEntity(data));
            response = httpclient.execute(httpPost);
        } catch (Exception ex) {
            throw new SpreadsheetAuthException(
                    "Error when attempting Http Request to OAuth2", ex);
        }

        if (response.getStatusLine().getStatusCode() != 200) {
            String msg = null;
            msg = response.getStatusLine().getStatusCode() + ": "
                    + response.getStatusLine().getReasonPhrase();
            throw new SpreadsheetAuthException(
                    "Error when attempting OAuth2 process: " + msg);
        }

        InputStream is = null;
        try {
            //Parse JSON response
            is = response.getEntity().getContent();
            GoogleJSONParser parser = new GoogleJSONParser();
            Map<?,?> jsonResponse = (Map<?, ?>) parser.parseObject(new InputStreamReader(is, Charset.forName(GoogleDataProtocolAPI.ENCODING)), false);

            //Report errors
            if (jsonResponse.get("error") != null){ //$NON-NLS-1$
                throw new SpreadsheetAuthException("OAuth2 service says: "+ jsonResponse.get("error")); //$NON-NLS-2$
            }
            return jsonResponse;

        } catch (IOException e) {
            throw new SpreadsheetAuthException(
                    "Error reading Client Login response", e);
        } finally {
            if (is != null)
                try {
                    is.close();
                } catch (IOException e) {
                }
        }
    }
}
