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
package org.teiid.resource.adapter.salesforce;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.cxf.jaxrs.client.WebClient;
import org.teiid.OAuthCredential;
import org.teiid.resource.adapter.salesforce.transport.SalesforceConnectorConfig;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class TeiidPartnerConnection extends PartnerConnection {

    private static final String AUTHORIZATION = "Authorization"; //$NON-NLS-1$

    public TeiidPartnerConnection(SalesforceConnectorConfig config) throws ConnectionException {
        super(config);
    }

    public com.sforce.soap.partner.LoginResult login(java.lang.String username,java.lang.String password)
            throws com.sforce.ws.ConnectionException {

        SalesforceConnectorConfig config = (SalesforceConnectorConfig)getConfig();
        if (config.getCredential(OAuthCredential.class.getName()) == null) {
            return super.login(username, password);
        }

        // for details see
        // https://developer.salesforce.com/blogs/developer-relations/2011/03/oauth-and-the-soap-api.html
        OAuthCredential credential = (OAuthCredential)config.getCredential(OAuthCredential.class.getName());
        String id = credential.getAuthrorizationProperty("id");
        if (id == null) {
            throw new com.sforce.ws.ConnectionException("Failed to get OAuth based connection");
        }
        String accessToken = credential.getAuthorizationHeader(null, "POST");

        com.sforce.soap.partner.LoginResult loginResult = null;
        WebClient client = WebClient.create(id);
        client.header(AUTHORIZATION, accessToken);
        String response = client.get(String.class);
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(new ByteArrayInputStream(response.getBytes()));
            doc.getDocumentElement().normalize();

            Element urls = (Element)doc.getDocumentElement().getElementsByTagName("urls").item(0);
            loginResult = new com.sforce.soap.partner.LoginResult();
            loginResult.setSessionId(accessToken.substring(7)); // remove "Bearer " prefix.

            String endpoint = config.getAuthEndpoint();
            int index = endpoint.indexOf("Soap/u/"); //$NON-NLS-1$
            String apiVersion = endpoint.substring(index+7);

            String partnerURL = urls.getElementsByTagName("partner").item(0).getTextContent();
            partnerURL = partnerURL.replace("{version}", apiVersion);

            loginResult.setServerUrl(partnerURL);

        } catch (IOException e) {
            throw new com.sforce.ws.ConnectionException("Failed to get OAuth based connection; "
                    + "Failed to get user information", e);
        } catch (ParserConfigurationException e) {
            throw new com.sforce.ws.ConnectionException("Failed to get OAuth based connection; "
                    + "Failed to get user information", e);
        } catch (IllegalStateException e) {
            throw new com.sforce.ws.ConnectionException("Failed to get OAuth based connection; "
                    + "Failed to get user information", e);
        } catch (SAXException e) {
            throw new com.sforce.ws.ConnectionException("Failed to get OAuth based connection; "
                    + "Failed to get user information", e);
        }

        return loginResult;
    }
}
