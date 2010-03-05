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
package org.teiid.connector.xml.http;

import com.metamatrix.connector.xml.base.XMLBaseManagedConnectionFactory;

public class HTTPManagedConnectionFactory extends XMLBaseManagedConnectionFactory {

	private String xMLParmName;

	public String getXMLParmName() {
		return this.xMLParmName;
	}

	public void setXMLParmName(String xMLParmName) {
		this.xMLParmName = xMLParmName;
	}

	private Integer requestTimeout;

	public Integer getRequestTimeout() {
		return this.requestTimeout;
	}

	public void setRequestTimeout(Integer requestTimeout) {
		this.requestTimeout = requestTimeout;
	}

	private boolean authenticate;

	public boolean getAuthenticate() {
		return this.authenticate;
	}

	public void setAuthenticate(Boolean authenticate) {
		this.authenticate = authenticate;
	}

	private String httpBasicAuthPassword;

	public String getHttpBasicAuthPassword() {
		return this.httpBasicAuthPassword;
	}

	public void setHttpBasicAuthPassword(String httpBasicAuthPassword) {
		this.httpBasicAuthPassword = httpBasicAuthPassword;
	}

	private String accessMethod;

	public String getAccessMethod() {
		return this.accessMethod;
	}

	public void setAccessMethod(String accessMethod) {
		this.accessMethod = accessMethod;
	}

	private String proxyUri;

	public String getProxyUri() {
		return this.proxyUri;
	}

	public void setProxyUri(String proxyUri) {
		this.proxyUri = proxyUri;
	}

	private String httpBasicAuthUserName;

	public String getHttpBasicAuthUserName() {
		return this.httpBasicAuthUserName;
	}

	public void setHttpBasicAuthUserName(String httpBasicAuthUserName) {
		this.httpBasicAuthUserName = httpBasicAuthUserName;
	}

	private String uri;

	public String getUri() {
		return this.uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	private boolean useHttpBasic;

	public boolean getUseHttpBasic() {
		return this.useHttpBasic;
	}

	public void setUseHttpBasic(Boolean useHttpBasic) {
		this.useHttpBasic = useHttpBasic;
	}

	private String parameterMethod;

	public String getParameterMethod() {
		return this.parameterMethod;
	}

	public void setParameterMethod(String parameterMethod) {
		this.parameterMethod = parameterMethod;
	}

	private String hostnameVerifier;

	public String getHostnameVerifier() {
		return this.hostnameVerifier;
	}

	public void setHostnameVerifier(String hostnameVerifier) {
		this.hostnameVerifier = hostnameVerifier;
	}
	
	private String trustDeserializerClass;

	public String getTrustDeserializerClass() {
		return this.trustDeserializerClass;
	}

	public void setTrustDeserializerClass(String trustDeserializerClass) {
		this.trustDeserializerClass = trustDeserializerClass;
	}	
	
	private String connectorStateClass;

	public String getConnectorStateClass() {
		return this.connectorStateClass;
	}

	public void setConnectorStateClass(String connectorStateClass) {
		this.connectorStateClass = connectorStateClass;
	}

	private boolean cacheEnabled;

	public boolean getCacheEnabled() {
		return this.cacheEnabled;
	}

	public void setCacheEnabled(Boolean cacheEnabled) {
		this.cacheEnabled = cacheEnabled;
	}
	
	private boolean logRequestResponseDocs;

	public boolean getLogRequestResponseDocs() {
		return this.logRequestResponseDocs;
	}

	public void setLogRequestResponseDocs(Boolean logRequestResponseDocs) {
		this.logRequestResponseDocs = logRequestResponseDocs;
	}	
}
