/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */


package com.metamatrix.connector.xml.http;

import java.util.Properties;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.httpclient.Credentials;
import org.apache.commons.httpclient.HostConfiguration;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.UsernamePasswordCredentials;
import org.apache.commons.httpclient.auth.AuthScope;
import org.apache.commons.httpclient.params.HttpConnectionManagerParams;
import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;

import com.metamatrix.connector.xml.CachingConnector;
import com.metamatrix.connector.xml.base.SecureConnectorStateImpl;

public class HTTPConnectorState extends SecureConnectorStateImpl {

    private int m_accessMethod;

    private int m_parameterMethod;

    private String m_uri;

    private String m_proxyUri;

    private int m_requestTimeout;

    private String m_xmlParameterName;

    private MultiThreadedHttpConnectionManager m_connMgr;

    private HttpConnectionManagerParams m_connMgrParams;

    private HttpClient m_client = null;

    private boolean m_useHttpBasicAuth;

    private String m_httpBasicAuthUser;

    private String m_httpBasicAuthPwd;
    
    private String hostnameVerifierClassName;

    private static final int METHOD_UNSET = -1;

    private static final int METHOD_GET = 0;

    public static final String GET = "GET"; //$NON-NLS-1$

    private static final int METHOD_POST = 1;

    public static final String POST = "POST"; //$NON-NLS-1$

    private static final int PARAMETER_METHOD_NONE = 0;

    public static final String PARAMETER_NONE = "None"; //$NON-NLS-1$

    private static final int PARAMETER_METHOD_NAME_VALUE = 1;

    public static final String PARAMETER_NAME_VALUE = "Name/Value"; //$NON-NLS-1$

    private static final int PARAMETER_METHOD_XML_REQUEST = 2;

    public static final String PARAMETER_XML_REQUEST = "XMLRequest"; //$NON-NLS-1$

    private static final int PARAMETER_METHOD_XML_QUERY_STRING = 3;

    public static final String PARAMETER_XML_QUERY_STRING = "XMLInQueryString"; //$NON-NLS-1$
    
    public static final String ACCESS_METHOD = "AccessMethod"; //$NON-NLS-1$

    public static final String PARAMETER_METHOD = "ParameterMethod"; //$NON-NLS-1$

    public static final String URI = "Uri"; //$NON-NLS-1$

    public static final String PROXY_URI = "ProxyUri"; //$NON-NLS-1$

    public static final String REQUEST_TIMEOUT = "RequestTimeout"; //$NON-NLS-1$

    public static final String XML_PARAMETER_NAME = "XMLParmName"; //$NON-NLS-1$

    public static final String USE_HTTP_BASIC_AUTH = "UseHttpBasic"; //$NON-NLS-1$

    public static final String HTTP_BASIC_USER = "HttpBasicAuthUserName"; //$NON-NLS-1$

    public static final String HTTP_BASIC_PASSWORD = "HttpBasicAuthPassword"; //$NON-NLS-1$
    
	public static final String HOSTNAME_VERIFIER = "HostnameVerifier"; //$NON-NLS-1$

    public HTTPConnectorState() {
        super();
        m_accessMethod = METHOD_UNSET;
        m_parameterMethod = METHOD_UNSET;
        m_uri = null;
        m_proxyUri = null;
        m_requestTimeout = -1;
        m_xmlParameterName = null;
        m_connMgr = null;
        m_connMgrParams = null;
        m_client = null;
        setUseHttpBasicAuth(false);
        setHttpBasicAuthUser(new String());
        setHttpBasicAuthPwd(new String());
    }

    @Override
	public void setState(ConnectorEnvironment env) throws ConnectorException {
        super.setState(env);
        Properties props = env.getProperties();
        setAccessMethod(props.getProperty(ACCESS_METHOD));
        setParameterMethod(props.getProperty(PARAMETER_METHOD));
        setUri(props.getProperty(URI));
        setProxyUri(props.getProperty(PROXY_URI));
        setRequestTimeout(Integer.parseInt(props.getProperty(REQUEST_TIMEOUT)));
        setXmlParameterName(props.getProperty(XML_PARAMETER_NAME));
        String basicAuth = props.getProperty(USE_HTTP_BASIC_AUTH);
        boolean useAuth = Boolean.valueOf(basicAuth).booleanValue();
        setUseHttpBasicAuth(useAuth);
        setHttpBasicAuthUser(props.getProperty(HTTP_BASIC_USER));
        setHttpBasicAuthPwd(props.getProperty(HTTP_BASIC_PASSWORD));
        
        setHostnameVerifierClassName(props.getProperty(HOSTNAME_VERIFIER));
        if(getHostnameVerifierClassName() != null) {
        	try {
        		Class clazz = Thread.currentThread().getContextClassLoader().loadClass(getHostnameVerifierClassName());
				HostnameVerifier verifier = (HostnameVerifier) clazz.newInstance();
				HttpsURLConnection.setDefaultHostnameVerifier(verifier);
			} catch (Exception e) {
				throw new ConnectorException(e, "Unable to load HostnameVerifier");
			}
        }
        initHttpClient();
    }

    /**
     * @param m_accessMethod
     *            The m_accessMethod to set.
     * @throws ConnectorException
     */
    public void setAccessMethod(String accessMethod) {
        m_accessMethod = encodeAccessMethod(accessMethod);
    }

    /**
     * @return Returns the m_accessMethod.
     */
    public String getAccessMethod() {
        return decodeAccessMethod(m_accessMethod);
    }

    /**
     * @param m_parameterMethod
     *            The m_parameterMethod to set.
     */
    public void setParameterMethod(String parameterMethod) {
        m_parameterMethod = encodeParameterMethod(parameterMethod);
    }

    /**
     * @return Returns the m_parameterMethod.
     */
    public String getParameterMethod() {
        return decodeParameterMethod(m_parameterMethod);
    }

    /**
     * @param m_uri
     *            The m_uri to set.
     */
    public void setUri(String uri) {
        m_uri = uri;
    }

    /**
     * @return Returns the m_uri.
     */
    public String getUri() {
        return m_uri;
    }

    /**
     * @param m_proxyUri
     *            The m_proxyUri to set.
     */
    public void setProxyUri(String proxyUri) {
        m_proxyUri = proxyUri;
    }

    /**
     * @return Returns the m_proxyUri.
     */
    public String getProxyUri() {
        return m_proxyUri;
    }

    /**
     * @param m_requestTimeout
     *            The m_requestTimeout to set.
     */
    public void setRequestTimeout(int requestTimeout) {
        m_requestTimeout = requestTimeout;
    }

    /**
     * @return Returns the m_requestTimeout.
     */
    public int getRequestTimeout() {
        return m_requestTimeout;
    }

    /**
     * @param m_xmlParameterName
     *            The m_xmlParameterName to set.
     */
    public void setXmlParameterName(String xmlParameterName) {
        m_xmlParameterName = xmlParameterName;
    }

    /**
     * @return Returns the m_xmlParameterName.
     */
    public String getXmlParameterName() {
        return m_xmlParameterName;
    }

    private int encodeAccessMethod(String method) {
        int retVal = METHOD_UNSET;
        if (method.equalsIgnoreCase(GET)) {
            retVal = METHOD_GET;
        } else {
            if (method.equalsIgnoreCase(POST)) {
                retVal = METHOD_POST;
            }
        }
        return retVal;
    }

    private String decodeAccessMethod(int method) {
        String retVal = ""; //$NON-NLS-1$
        switch (method) {
        case METHOD_GET:
            retVal = GET;
            break;
        case METHOD_POST:
            retVal = POST;
            break;
        default:
        }
        return retVal;
    }

    private int encodeParameterMethod(String method) {
        int retVal = METHOD_UNSET;
        if (method.equalsIgnoreCase(PARAMETER_NONE)) {
            retVal = PARAMETER_METHOD_NONE;
        } else {
            if (method.equalsIgnoreCase(PARAMETER_NAME_VALUE)) {
                retVal = PARAMETER_METHOD_NAME_VALUE;
            } else {
                if (method.equalsIgnoreCase(PARAMETER_XML_REQUEST)) {
                    retVal = PARAMETER_METHOD_XML_REQUEST;
                } else {
                    if (method.equalsIgnoreCase(PARAMETER_XML_QUERY_STRING)) {
                        retVal = PARAMETER_METHOD_XML_QUERY_STRING;
                    }
                }

            }
        }
        return retVal;
    }

    private String decodeParameterMethod(int method) {
        String retVal = ""; //$NON-NLS-1$
        switch (method) {
        case PARAMETER_METHOD_NONE:
            retVal = PARAMETER_NONE;
            break;
        case PARAMETER_METHOD_NAME_VALUE:
            retVal = PARAMETER_NAME_VALUE;
            break;
        case PARAMETER_METHOD_XML_REQUEST:
            retVal = PARAMETER_XML_REQUEST;
            break;
        case PARAMETER_METHOD_XML_QUERY_STRING:
            retVal = PARAMETER_XML_QUERY_STRING;
            break;
        default:
        }
        return retVal;
    }

    public HttpClient getClient() throws ConnectorException {
        if(null == m_client) {
        	initHttpClient();
        }
        return m_client;
    }

    private void initHttpClient() throws ConnectorException {
        m_connMgr = new MultiThreadedHttpConnectionManager();
        m_connMgrParams = new HttpConnectionManagerParams();
        final int threads = 10; // change
        m_connMgrParams.setDefaultMaxConnectionsPerHost(threads);
        m_connMgrParams.setMaxTotalConnections(threads);
        m_connMgrParams.setConnectionTimeout(0);
        m_connMgrParams.setSoTimeout(getRequestTimeout());
        m_connMgr.setParams(m_connMgrParams);
        m_client = new HttpClient(m_connMgr);
        HostConfiguration hc = null;
        try {
            URI uri = new URI(getUri(), false); // false = not escaped
            hc = m_client.getHostConfiguration();
            hc.setHost(uri);
            String proxyUriStr = getProxyUri();
            if (proxyUriStr != null && proxyUriStr != "") {
                URI proxyUri = new URI(proxyUriStr, false);
                int pp = proxyUri.getPort();
                int substrEnd = getProxyUri().lastIndexOf(":"); //$NON-NLS-1$
                String hostPart = null;
                if (substrEnd == -1) {
                    hostPart = getProxyUri();
                } else {
                    hostPart = getProxyUri().substring(0, substrEnd);
                }
                if (pp >= 0) {
                    hc.setProxy(hostPart, pp);
                } else {
                    final int defaultProxyPort = 80;
                    hc.setProxy(hostPart, defaultProxyPort);
                }
            }
        } catch (URIException uex) {
            ConnectorException ce = new ConnectorException(
                    com.metamatrix.connector.xml.http.Messages
                            .getString("HTTPConnectorState.connection.not.initialized") + getUri()); //$NON-NLS-1$  
            ce.initCause(uex);
            throw ce;
        }

        m_client.setHostConfiguration(hc);
        if (useHttpBasicAuth()) {
            AuthScope authScope = new AuthScope(null, -1); // Create AuthScope
                                                            // for any host
                                                            // (null) and any
                                                            // port (-1).
            Credentials defCred = new UsernamePasswordCredentials(
                    getHttpBasicAuthUser(), getHttpBasicAuthPwd());
            m_client.getState().setCredentials(authScope, defCred);
            m_client.getParams().setAuthenticationPreemptive(true);
        }
    }

    public void setUseHttpBasicAuth(boolean m_useHttpBasicAuth) {
        this.m_useHttpBasicAuth = m_useHttpBasicAuth;
    }

    public boolean useHttpBasicAuth() {
        return m_useHttpBasicAuth;
    }

    public void setHttpBasicAuthUser(String m_httpBasicAuthUser) {
        this.m_httpBasicAuthUser = m_httpBasicAuthUser;
    }

    public String getHttpBasicAuthUser() {
        return m_httpBasicAuthUser;
    }

    public void setHttpBasicAuthPwd(String m_httpBasicAuthPwd) {
        this.m_httpBasicAuthPwd = m_httpBasicAuthPwd;
    }

    public String getHttpBasicAuthPwd() {
        return m_httpBasicAuthPwd;
    }

    public Connection getConnection(CachingConnector connector,
            ExecutionContext context, ConnectorEnvironment environment)
            throws ConnectorException {
        return new HTTPConnectionImpl(connector, context, environment);
    }
    
    private void setHostnameVerifierClassName(String property) {
		this.hostnameVerifierClassName = property;
	}
	
	private String getHostnameVerifierClassName() {
		return hostnameVerifierClassName;
	}
}
