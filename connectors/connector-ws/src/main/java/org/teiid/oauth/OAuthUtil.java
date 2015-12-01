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
        org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils.Consumer consumer = new org.apache.cxf.rs.security.oauth2.client.OAuthClientUtils.Consumer(clientID,clientSecret);
        
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
                return null;
            }
        }
    }
}
