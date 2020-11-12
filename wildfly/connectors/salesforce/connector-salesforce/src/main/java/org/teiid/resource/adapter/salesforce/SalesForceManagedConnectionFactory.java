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

import java.lang.reflect.Field;

import javax.resource.ResourceException;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.salesforce.SalesforceConfiguration;
import org.teiid.translator.salesforce.SalesForcePlugin;

import com.sforce.async.AsyncApiException;
import com.sforce.soap.partner.Connector;
import com.sforce.ws.ConnectionException;


public class SalesForceManagedConnectionFactory extends BasicManagedConnectionFactory implements SalesforceConfiguration {

    private static final long serialVersionUID = 5298591275313314698L;

    private String username;
    private String password;
    private String url; //sf url
    private Long requestTimeout;
    private Long connectTimeout;

    private String proxyUsername;
    private String proxyPassword;
    private String proxyUrl;

    private String configProperties;
    private String configFile; // path to the "jbossws-cxf.xml" file

    @Override
    public String getUsername() {
        return username;
    }
    public void setUsername(String username) {
        if (username.trim().length() == 0) {
            throw new TeiidRuntimeException("Name can not be null"); //$NON-NLS-1$
        }
        this.username = username;
    }

    @Override
    public String getPassword() {
        return this.password;
    }
    public void setPassword(String password) {
        this.password = password;
    }

    @Override
    public String getURL() {
        return this.url;
    }

    public void setURL(String uRL) {
        this.url = uRL;
    }

    @Override
    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public Long getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(Long requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory() throws ResourceException {
        checkVersion();
        return new BasicConnectionFactory<ResourceConnection>() {
            private static final long serialVersionUID = 5028356110047329135L;

            @Override
            public ResourceConnection getConnection() throws ResourceException {
                try {
                    return new SalesforceConnectionImpl(SalesForceManagedConnectionFactory.this);
                } catch (AsyncApiException | ConnectionException e) {
                    throw new ResourceException(e);
                }
            }
        };
    }

    public String getProxyUsername() {
        return proxyUsername;
    }

    public void setProxyUsername(String proxyUsername) {
        this.proxyUsername = proxyUsername;
    }

    public String getProxyPassword() {
        return proxyPassword;
    }

    public void setProxyPassword(String proxyPassword) {
        this.proxyPassword = proxyPassword;
    }

    public String getProxyURL() {
        return proxyUrl;
    }

    public void setProxyURL(String proxyUrl) {
        this.proxyUrl = proxyUrl;
    }

    public String getConfigProperties() {
        return configProperties;
    }

    public void setConfigProperties(String configProperties) {
        this.configProperties = configProperties;
    }

    public String getConfigFile() {
        return this.configFile;
    }

    public void setConfigFile(String config) {
        this.configFile = config;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((configFile == null) ? 0 : configFile.hashCode());
        result = prime * result + ((configProperties == null) ? 0
                : configProperties.hashCode());
        result = prime * result
                + ((connectTimeout == null) ? 0 : connectTimeout.hashCode());
        result = prime * result
                + ((password == null) ? 0 : password.hashCode());
        result = prime * result
                + ((proxyPassword == null) ? 0 : proxyPassword.hashCode());
        result = prime * result
                + ((proxyUrl == null) ? 0 : proxyUrl.hashCode());
        result = prime * result
                + ((proxyUsername == null) ? 0 : proxyUsername.hashCode());
        result = prime * result
                + ((requestTimeout == null) ? 0 : requestTimeout.hashCode());
        result = prime * result + ((url == null) ? 0 : url.hashCode());
        result = prime * result
                + ((username == null) ? 0 : username.hashCode());
        return result;
    }
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SalesForceManagedConnectionFactory other = (SalesForceManagedConnectionFactory) obj;
        if (configFile == null) {
            if (other.configFile != null)
                return false;
        } else if (!configFile.equals(other.configFile))
            return false;
        if (configProperties == null) {
            if (other.configProperties != null)
                return false;
        } else if (!configProperties.equals(other.configProperties))
            return false;
        if (connectTimeout == null) {
            if (other.connectTimeout != null)
                return false;
        } else if (!connectTimeout.equals(other.connectTimeout))
            return false;
        if (password == null) {
            if (other.password != null)
                return false;
        } else if (!password.equals(other.password))
            return false;
        if (proxyPassword == null) {
            if (other.proxyPassword != null)
                return false;
        } else if (!proxyPassword.equals(other.proxyPassword))
            return false;
        if (proxyUrl == null) {
            if (other.proxyUrl != null)
                return false;
        } else if (!proxyUrl.equals(other.proxyUrl))
            return false;
        if (proxyUsername == null) {
            if (other.proxyUsername != null)
                return false;
        } else if (!proxyUsername.equals(other.proxyUsername))
            return false;
        if (requestTimeout == null) {
            if (other.requestTimeout != null)
                return false;
        } else if (!requestTimeout.equals(other.requestTimeout))
            return false;
        if (url == null) {
            if (other.url != null)
                return false;
        } else if (!url.equals(other.url))
            return false;
        if (username == null) {
            if (other.username != null)
                return false;
        } else if (!username.equals(other.username))
            return false;
        return true;
    }

    public void checkVersion() {
        String urlToCheck = url;
        if (url.endsWith("/")) { //$NON-NLS-1$
            urlToCheck = url.substring(0, url.length()-1);
        }
        String apiVersion = urlToCheck.substring(urlToCheck.lastIndexOf('/') + 1, urlToCheck.length());
        String javaApiVersion = getJavaApiVersion();
        if (javaApiVersion != null && !javaApiVersion.equals(apiVersion)) {
            LogManager.logWarning(LogConstants.CTX_CONNECTOR, SalesForcePlugin.Util.gs(SalesForcePlugin.Event.TEIID13009, apiVersion, javaApiVersion));
        }
    }

    public static String getJavaApiVersion() {
        try {
            Field f = Connector.class.getDeclaredField("END_POINT"); //$NON-NLS-1$
            f.setAccessible(true);
            if(f.isAccessible()){
                String endPoint = (String) f.get(null);
                return endPoint.substring(endPoint.lastIndexOf('/') + 1, endPoint.length());
            }
        } catch (Exception e) {

        }
        return null;
    }

}
