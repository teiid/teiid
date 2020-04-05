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

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import javax.security.auth.Subject;

import org.teiid.OAuthCredential;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.adapter.salesforce.transport.SalesforceCXFTransport;
import org.teiid.resource.adapter.salesforce.transport.SalesforceConnectorConfig;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.salesforce.BaseSalesforceConnection;

import com.sforce.async.AsyncApiException;
import com.sforce.soap.partner.PartnerConnection;
import com.sforce.ws.ConnectionException;

public class SalesforceConnectionImpl extends BaseSalesforceConnection<SalesForceManagedConnectionFactory> implements ResourceConnection {

    public SalesforceConnectionImpl(
            SalesForceManagedConnectionFactory config) throws AsyncApiException, ConnectionException {
        super(config);
    }

    public SalesforceConnectionImpl(
            PartnerConnection partnerConnection) {
        super(partnerConnection);
    }

    @Override
    protected void login(SalesForceManagedConnectionFactory mcf) throws AsyncApiException, ConnectionException {
        SalesforceConnectorConfig salesforceConnectorConfig = new SalesforceConnectorConfig();
        config = salesforceConnectorConfig;
        String username = mcf.getUsername();
        String password = mcf.getPassword();

        // if security-domain is specified and caller identity is used; then use
        // credentials from subject
        boolean useCXFTransport = mcf.getConfigFile() != null;
        Subject subject = ConnectionContext.getSubject();
        if (subject != null) {
            OAuthCredential oauthCredential = ConnectionContext.getSecurityCredential(subject, OAuthCredential.class);
            if (oauthCredential != null) {
                salesforceConnectorConfig.setCredential(OAuthCredential.class.getName(), oauthCredential);
                useCXFTransport = true;
            } else {
                username = ConnectionContext.getUserName(subject, mcf, username);
                password = ConnectionContext.getPassword(subject, mcf, username, password);
            }
        }

        salesforceConnectorConfig.setCxfConfigFile(mcf.getConfigFile());
        if (useCXFTransport) {
            config.setTransport(SalesforceCXFTransport.class);
        }

        config.setCompression(true);
        config.setTraceMessage(false);

        //set the catch all properties
        String props = mcf.getConfigProperties();
        if (props != null) {
            Properties p = new Properties();
            try {
                p.load(new StringReader(props));
            } catch (IOException e) {
                throw new ConnectionException("Could not read config properties", e); //$NON-NLS-1$
            }
            PropertiesUtils.setBeanProperties(config, p, null);
        }

        config.setUsername(username);
        config.setPassword(password);
        config.setAuthEndpoint(mcf.getURL());

        //set proxy if needed
        if (mcf.getProxyURL() != null) {
            try {
                URL proxyURL = new URL(mcf.getProxyURL());
                config.setProxy(proxyURL.getHost(), proxyURL.getPort());
                config.setProxyUsername(mcf.getProxyUsername());
                config.setProxyPassword(mcf.getProxyPassword());
            } catch (MalformedURLException e) {
                throw new ConnectionException(e.getMessage(), e);
            }
        }
        if (mcf.getConnectTimeout() != null) {
            config.setConnectionTimeout((int) Math.min(Integer.MAX_VALUE, mcf.getConnectTimeout()));
        }
        if (mcf.getRequestTimeout() != null) {
            config.setReadTimeout((int) Math.min(Integer.MAX_VALUE, mcf.getRequestTimeout()));
        }

        partnerConnection = new TeiidPartnerConnection(salesforceConnectorConfig);

        LogManager.logTrace(LogConstants.CTX_CONNECTOR, "Login was successful for username", username); //$NON-NLS-1$
    }

}