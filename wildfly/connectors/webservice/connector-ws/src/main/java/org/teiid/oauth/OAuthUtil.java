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
package org.teiid.oauth;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Scanner;

import org.apache.cxf.jaxrs.client.WebClient;
import org.apache.cxf.jaxrs.provider.FormEncodingProvider;
import org.apache.cxf.rs.security.oauth.client.OAuthClientUtils;
import org.apache.cxf.rs.security.oauth2.common.AccessTokenGrant;
import org.apache.cxf.rs.security.oauth2.common.ClientAccessToken;
import org.apache.cxf.rs.security.oauth2.grants.code.AuthorizationCodeGrant;

@SuppressWarnings("nls")
public class OAuthUtil {
    public static final String OAUTH1_0_DOMAIN =
            "<security-domain name=\"oauth-security\">  \n" +
            "    <authentication>  \n" +
            "        <login-module code=\"org.teiid.jboss.oauth.OAuth10LoginModule\" flag=\"required\" module=\"org.jboss.teiid.security\">  \n" +
            "            <module-option name=\"consumer-key\" value=\"{0}\"/>  \n" +
            "            <module-option name=\"consumer-secret\" value=\"{1}\"/>  \n" +
            "            <module-option name=\"access-key\" value=\"{2}\"/>  \n" +
            "            <module-option name=\"access-secret\" value=\"{3}\"/>  \n" +
            "        </login-module>  \n" +
            "    </authentication>  \n" +
            "</security-domain> ";

    public static final String OAUTH2_0_DOMAIN =
            "<security-domain name=\"oauth2-security\">  \n" +
            "    <authentication>  \n" +
            "        <login-module code=\"org.teiid.jboss.oauth.OAuth20LoginModule\" flag=\"required\" module=\"org.jboss.teiid.security\">  \n" +
            "            <module-option name=\"client-id\" value=\"{0}\"/>  \n" +
            "            <module-option name=\"client-secret\" value=\"{1}\"/>  \n" +
            "            <module-option name=\"refresh-token\" value=\"{2}\"/>  \n" +
            "            <module-option name=\"access-token-uri\" value=\"{3}\"/>  \n" +
            "        </login-module>  \n" +
            "    </authentication>  \n" +
            "</security-domain> ";

    public static void main(String[] args) throws Exception {
        Scanner in = new Scanner(System.in);

        System.out.println("Select type of OAuth authentication");
        System.out.println("1) OAuth 1.0A");
        System.out.println("2) OAuth 2.0");
        System.out.println("3) OAuth 2.0 - Facebook");
        System.out.println();

        String input = in.nextLine();
        input = input.trim();

        switch(Integer.parseInt(input)) {
        case 1:
            oauth10Flow(in);
            break;
        case 2:
            oauth20Flow(in);
            break;
        case 3:
            oauth20FlowFacebook(in);
        }
        in.close();
    }

    private static void oauth10Flow(Scanner in) throws Exception, URISyntaxException {
        System.out.println("=== OAuth 1.0a Workflow ===");
        System.out.println();

        String consumerKey = getInput(in, "Enter the Consumer Key = ");
        String consumerSecret = getInput(in, "Enter the Consumer Secret = ");
        OAuthClientUtils.Consumer consumer = new OAuthClientUtils.Consumer(consumerKey,consumerSecret);

        String requestURL = getInput(in, "Enter the Request Token URL = ");

        FormEncodingProvider<?> provider = new FormEncodingProvider<Object>();
        provider.setConsumeMediaTypes(Arrays.asList("text/html"));

        WebClient client = WebClient.create(requestURL, Arrays.asList(provider));

        OAuthClientUtils.Token requestToken = OAuthClientUtils.getRequestToken(client, consumer, new URI("oob"), null);
        System.out.println("Request Token  = " + requestToken.getToken() + " secret = " + requestToken.getSecret());
        System.out.println("");

        String authorizeURL = getInput(in, "Enter the User Authorization URL = ");

        URI authenticateURL = OAuthClientUtils.getAuthorizationURI(authorizeURL, requestToken.getToken());

        System.out.println("Cut & Paste the URL in a web browser, and Authticate");
        System.out.println("Authorize URL  = " + authenticateURL);
        System.out.println("");

        String authCode = getInput(in, "Enter Token Secret (Auth Code, Pin) from previous step = ");

        String accessTokenURL = getInput(in, "Enter the Access Token URL = ");
        client = WebClient.create(accessTokenURL, Arrays.asList(provider));

        OAuthClientUtils.Token accessToken = OAuthClientUtils.getAccessToken(client, consumer,requestToken, authCode);
        System.out.println("Access Token = " + accessToken.getToken() + " Secret = " + accessToken.getSecret());
        System.out.println("");
        System.out.println("Add the following XML into your standalone-teiid.xml file in security-domains subsystem,\n"
                + "and configure data source securty to this domain");
        System.out.println("");
        System.out.println("");
        System.out.println(MessageFormat.format(OAUTH1_0_DOMAIN, consumerKey, consumerSecret, accessToken.getToken(), accessToken.getSecret()));
    }

    private static void oauth20Flow(Scanner in) throws Exception {
        System.out.println("=== OAuth 2.0 Workflow ===");
        System.out.println();

        String clientID = getInput(in, "Enter the Client ID = ");
        String clientSecret = getInput(in, "Enter the Client Secret = ");
        org.apache.cxf.rs.security.oauth2.client.Consumer consumer = new org.apache.cxf.rs.security.oauth2.client.Consumer(clientID,clientSecret);

        String authorizeURL = getInput(in, "Enter the User Authorization URL = ");
        String scope = getInput(in, "Enter scope (hit enter for none) = ", true);

        String callback = getInput(in, "Enter callback URL (default: urn:ietf:wg:oauth:2.0:oob) = ", true);
        if (callback == null) {
            callback = "urn:ietf:wg:oauth:2.0:oob";
        }

        URI authenticateURL = org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils.getAuthorizationURI(authorizeURL,
                consumer.getKey(),
                callback,
                "Auth URL",
                scope);

        System.out.println("Cut & Paste the URL in a web browser, and Authticate");
        System.out.println("Authorize URL  = " + authenticateURL.toASCIIString());
        System.out.println("");

        String authCode = getInput(in, "Enter Token Secret (Auth Code, Pin) from previous step = ");

        String accessTokenURL = getInput(in, "Enter the Access Token URL = ");
        WebClient client = WebClient.create(accessTokenURL);

        AccessTokenGrant grant = new AuthorizationCodeGrant(authCode, new URI(callback));
        ClientAccessToken clientToken = org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils.getAccessToken(client, consumer, grant, null, false);
        System.out.println("Refresh Token="+clientToken.getRefreshToken());

        System.out.println("");
        System.out.println("Add the following XML into your standalone-teiid.xml file in security-domains subsystem,\n"
                + "and configure data source securty to this domain");
        System.out.println("");
        System.out.println("");
        System.out.println(MessageFormat.format(OAUTH2_0_DOMAIN, clientID, clientSecret,
                clientToken.getRefreshToken(), accessTokenURL));
    }

    private static void oauth20FlowFacebook(Scanner in) throws Exception {
        System.out.println("=== OAuth 2.0 - Facebook Workflow ===");
        System.out.println();

        String clientID = getInput(in, "Enter the App ID = ");
        String clientSecret = getInput(in, "Enter the App Secret = ");
        org.apache.cxf.rs.security.oauth2.client.Consumer consumer = new org.apache.cxf.rs.security.oauth2.client.Consumer(clientID,clientSecret);

        String authorizeURL = getInput(in, "Enter the User Authorization URL(default: https://www.facebook.com/v2.9/dialog/oauth) = ", true);
        if(authorizeURL == null) {
            authorizeURL = "https://www.facebook.com/v2.9/dialog/oauth";
        }

        String scope = getInput(in, "Enter scope (default: public_profile,user_friends,email,pages_manage_cta) = ", true);
        if(scope == null) {
            scope = "public_profile,user_friends,email,pages_manage_cta";
        }

        String callback = getInput(in, "Enter callback URL (the redirect url match the setting in your Facebook App) = ");

        URI authenticateURL = org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils.getAuthorizationURI(authorizeURL,
                consumer.getKey(),
                callback,
                "Auth URL",
                scope);

        System.out.println("Cut & Paste the URL in a web browser, and Authticate");
        System.out.println("Authorize URL  = " + authenticateURL.toASCIIString());
        System.out.println("");

        String authCode = getInput(in, "Enter Token Secret (Auth Code) from previous step = ");

        String accessTokenURL = getInput(in, "Enter the Access Token URL(default: https://graph.facebook.com/v2.9/oauth/access_token) = ", true);
        if(accessTokenURL == null) {
            accessTokenURL = "https://graph.facebook.com/v2.9/oauth/access_token";
        }

        System.out.println("Cut & Paste the URL in a web browser, and Get Access Token");
        System.out.println("Access Token URL  = " + accessTokenURL + "?client_id=" + clientID + "&client_secret=" + clientSecret + "&redirect_uri=" + callback + "&code=" + authCode);
        System.out.println("");

        String access_token = getInput(in, "Enter access_token from previous step = ");

        System.out.println("");
        System.out.println("Add the following XML into your standalone-teiid.xml file in security-domains subsystem,\n"
                + "and configure data source securty to this domain");
        System.out.println("");
        System.out.println("");
        System.out.println(MessageFormat.format(OAUTH2_0_DOMAIN, clientID, clientSecret, access_token, accessTokenURL));
    }

    public static String getInput(Scanner in, String message) throws Exception {
        return getInput(in, message, false);
    }

    public static String getInput(Scanner in, String message, boolean allowNull) throws Exception {
        while (true) {
            System.out.print(message);
            String input = in.nextLine();
            input = input.trim();
            if (input.length() > 1) {
                System.out.println("");
                return input;
            }

            if (allowNull) {
                System.out.println("");
                return null;
            }
        }
    }
}
