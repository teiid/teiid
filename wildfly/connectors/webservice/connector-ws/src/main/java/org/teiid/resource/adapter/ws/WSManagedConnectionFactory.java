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
package org.teiid.resource.adapter.ws;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ConnectionContext;
import org.teiid.resource.spi.ResourceConnection;
import org.teiid.translator.TranslatorException;
import org.teiid.ws.cxf.BaseWSConnection;
import org.teiid.ws.cxf.WSConfiguration;
import org.teiid.ws.cxf.WSConnectionFactory;

public class WSManagedConnectionFactory extends BasicManagedConnectionFactory implements WSConfiguration {

    private class WSConnectionImpl extends BaseWSConnection implements ResourceConnection {

        public WSConnectionImpl(WSConnectionFactory wsConnectionFactory) {
            super(wsConnectionFactory);
        }

        @Override
        protected String getUserName(Subject s, String defaultUserName) {
            return ConnectionContext.getUserName(s, WSManagedConnectionFactory.this, defaultUserName);
        }

        @Override
        protected Subject getSubject() {
            return ConnectionContext.getSubject();
        }

        @Override
        protected <T> T getSecurityCredential(Subject s, Class<T> clazz) {
            return ConnectionContext.getSecurityCredential(s, clazz);
        }

        @Override
        protected String getPassword(Subject s, String userName,
                String defaultPassword) {
            return ConnectionContext.getPassword(s, WSManagedConnectionFactory.this, userName, defaultPassword);
        }
    }

    private static final long serialVersionUID = -2998163922934555003L;

    //ws properties
    private String endPoint;

    //wsdl properties
    private String wsdl;
    private String serviceName = WSConfiguration.DEFAULT_LOCAL_NAME;

    //shared properties
    private String securityType = SecurityType.None.name(); // None, HTTPBasic, WS-Security
    private String configFile; // path to the "jbossws-cxf.xml" file
    private String endPointName = WSConfiguration.DEFAULT_LOCAL_NAME;
    private String authPassword; // httpbasic - password
    private String authUserName; // httpbasic - username
    private Long requestTimeout;
    private Long connectTimeout;
    private String namespaceUri = WSConfiguration.DEFAULT_NAMESPACE_URI;

    @SuppressWarnings("serial")
    @Override
    public BasicConnectionFactory<ResourceConnection> createConnectionFactory() throws ResourceException {
        try {
            WSConnectionFactory wsConnectionFactory = new WSConnectionFactory(this);
            return new BasicConnectionFactory<ResourceConnection>() {
                @Override
                public ResourceConnection getConnection() throws ResourceException {
                    return new WSConnectionImpl(wsConnectionFactory);
                }

            };
        } catch (TranslatorException e) {
            throw new ResourceException(e);
        }
    }

    @Override
    public String getNamespaceUri() {
        return this.namespaceUri;
    }

    public void setNamespaceUri(String namespaceUri) {
        this.namespaceUri = namespaceUri;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    @Override
    public String getEndPointName() {
        return this.endPointName;
    }

    public void setEndPointName(String portName) {
        this.endPointName = portName;
    }

    @Override
    public String getAuthPassword() {
        return this.authPassword;
    }

    public void setAuthPassword(String authPassword) {
        this.authPassword = authPassword;
    }

    @Override
    public String getAuthUserName() {
        return this.authUserName;
    }

    public void setAuthUserName(String authUserName) {
        this.authUserName = authUserName;
    }

    @Override
    public String getEndPoint() {
        return this.endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    @Override
    public Long getRequestTimeout() {
        return this.requestTimeout;
    }

    public void setRequestTimeout(Long timeout) {
        this.requestTimeout = timeout;
    }

    @Override
    public String getSecurityType() {
        return this.securityType;
    }

    public void setSecurityType(String securityType) {
        this.securityType = securityType;
    }

    @Override
    public String getConfigFile() {
        return this.configFile;
    }

    public void setConfigFile(String config) {
        this.configFile = config;
    }

    @Override
    public String getConfigName() {
        return this.endPointName;
    }

    public void setConfigName(String configName) {
        this.endPointName = configName;
    }

    @Override
    public String getWsdl() {
        return this.wsdl;
    }

    public void setWsdl(String wsdl) {
        this.wsdl = wsdl;
    }

    @Override
    public Long getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(Long connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((authPassword == null) ? 0 : authPassword.hashCode());
        result = prime * result
                + ((authUserName == null) ? 0 : authUserName.hashCode());
        result = prime * result
                + ((configFile == null) ? 0 : configFile.hashCode());
        result = prime * result
                + ((connectTimeout == null) ? 0 : connectTimeout.hashCode());
        result = prime * result
                + ((endPoint == null) ? 0 : endPoint.hashCode());
        result = prime * result
                + ((endPointName == null) ? 0 : endPointName.hashCode());
        result = prime * result
                + ((namespaceUri == null) ? 0 : namespaceUri.hashCode());
        result = prime * result
                + ((requestTimeout == null) ? 0 : requestTimeout.hashCode());
        result = prime * result
                + ((securityType == null) ? 0 : securityType.hashCode());
        result = prime * result
                + ((serviceName == null) ? 0 : serviceName.hashCode());
        result = prime * result + ((wsdl == null) ? 0 : wsdl.hashCode());
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
        WSManagedConnectionFactory other = (WSManagedConnectionFactory) obj;
        if (authPassword == null) {
            if (other.authPassword != null)
                return false;
        } else if (!authPassword.equals(other.authPassword))
            return false;
        if (authUserName == null) {
            if (other.authUserName != null)
                return false;
        } else if (!authUserName.equals(other.authUserName))
            return false;
        if (configFile == null) {
            if (other.configFile != null)
                return false;
        } else if (!configFile.equals(other.configFile))
            return false;
        if (connectTimeout == null) {
            if (other.connectTimeout != null)
                return false;
        } else if (!connectTimeout.equals(other.connectTimeout))
            return false;
        if (endPoint == null) {
            if (other.endPoint != null)
                return false;
        } else if (!endPoint.equals(other.endPoint))
            return false;
        if (endPointName == null) {
            if (other.endPointName != null)
                return false;
        } else if (!endPointName.equals(other.endPointName))
            return false;
        if (namespaceUri == null) {
            if (other.namespaceUri != null)
                return false;
        } else if (!namespaceUri.equals(other.namespaceUri))
            return false;
        if (requestTimeout == null) {
            if (other.requestTimeout != null)
                return false;
        } else if (!requestTimeout.equals(other.requestTimeout))
            return false;
        if (securityType == null) {
            if (other.securityType != null)
                return false;
        } else if (!securityType.equals(other.securityType))
            return false;
        if (serviceName == null) {
            if (other.serviceName != null)
                return false;
        } else if (!serviceName.equals(other.serviceName))
            return false;
        if (wsdl == null) {
            if (other.wsdl != null)
                return false;
        } else if (!wsdl.equals(other.wsdl))
            return false;
        return true;
    }
}
