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
package org.teiid.resource.adapter.salesforce;

import javax.resource.ResourceException;
import javax.security.auth.Subject;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;
import org.teiid.resource.spi.ConnectionContext;


public class SalesForceManagedConnectionFactory extends BasicManagedConnectionFactory {
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

	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		if (username.trim().length() == 0) {
			throw new TeiidRuntimeException("Name can not be null"); //$NON-NLS-1$
		}
		this.username = username;
	}

	public String getPassword() {
		return this.password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	
	public String getURL() {
		return this.url;
	}
	
	public void setURL(String uRL) {
		this.url = uRL;
	}
	
	public Long getConnectTimeout() {
		return connectTimeout;
	}
	
	public void setConnectTimeout(Long connectTimeout) {
		this.connectTimeout = connectTimeout;
	}

	public Long getRequestTimeout() {
		return requestTimeout;
	}
	
	public void setRequestTimeout(Long requestTimeout) {
		this.requestTimeout = requestTimeout;
	}
	
	@Override
	public BasicConnectionFactory<SalesforceConnectionImpl> createConnectionFactory() throws ResourceException {
		
		return new BasicConnectionFactory<SalesforceConnectionImpl>() {
			private static final long serialVersionUID = 5028356110047329135L;

			@Override
			public SalesforceConnectionImpl getConnection() throws ResourceException {				
				return new SalesforceConnectionImpl(SalesForceManagedConnectionFactory.this);
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
		result = prime * result + ((url == null) ? 0 : url.hashCode());
		result = prime * result + ((password == null) ? 0 : password.hashCode());
		result = prime * result + ((username == null) ? 0 : username.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj) return true;
		if (obj == null) return false;
		if (getClass() != obj.getClass()) return false;
		SalesForceManagedConnectionFactory other = (SalesForceManagedConnectionFactory) obj;
		if (!checkEquals(this.url, other.url)) {
			return false;
		}
		if (!checkEquals(this.password, other.password)) {
			return false;
		}
		if (!checkEquals(this.username, other.username)) {
			return false;
		}
		return checkEquals(this.proxyUrl, other.proxyUrl) 
				&& checkEquals(this.proxyUsername, other.proxyUsername)
				&& checkEquals(this.proxyPassword, other.proxyPassword)
				&& checkEquals(this.configProperties, other.configProperties);
	}
	
}
