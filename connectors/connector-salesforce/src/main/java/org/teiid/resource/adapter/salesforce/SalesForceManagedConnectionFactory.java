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

import java.net.MalformedURLException;
import java.net.URL;

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
	private URL URL; //sf url
	private Long requestTimeout;
	private Long connectTimeout;

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
	public URL getAsURL() {
		return this.URL;
	}
	
	public String getURL() {
		return this.URL.toExternalForm();
	}
	
	public void setURL(String uRL) {
		try {
			this.URL = new URL(uRL);
		} catch (MalformedURLException e) {
			throw new TeiidRuntimeException("URL Supplied is not valid URL"+ e.getMessage());//$NON-NLS-1$
		}
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
				String userName = getUsername();
				String password = getPassword();

				// if security-domain is specified and caller identity is used; then use
				// credentials from subject
				Subject subject = ConnectionContext.getSubject();
				if (subject != null) {
					userName = ConnectionContext.getUserName(subject, SalesForceManagedConnectionFactory.this, userName);
					password = ConnectionContext.getPassword(subject, SalesForceManagedConnectionFactory.this, userName, password);
				}
				
				return new SalesforceConnectionImpl(userName, password, getAsURL(), SalesForceManagedConnectionFactory.this);
			}
		};
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((URL == null) ? 0 : URL.hashCode());
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
		if (!checkEquals(this.URL, other.URL)) {
			return false;
		}
		if (!checkEquals(this.password, other.password)) {
			return false;
		}
		if (!checkEquals(this.username, other.username)) {
			return false;
		}
		return true;
	}
}
