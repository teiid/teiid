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
package org.teiid.resource.cci.salesforce;

import java.net.MalformedURLException;
import java.net.URL;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import com.metamatrix.core.MetaMatrixRuntimeException;

public class SalesForceManagedConnectionFactory extends BasicManagedConnectionFactory {
	private static final long serialVersionUID = 5298591275313314698L;
	
	private String username;
	private String connectorStateClass;
	private String password;
	private URL URL;
	private long sourceConnectionTestInterval = -1;
	private int sourceConnectionTimeout = -1;
	private boolean auditModelFields = false;
	
	public String getUsername() {
		return username;
	}
	public void setUsername(String username) {
		if (username.trim().length() == 0) {
			throw new MetaMatrixRuntimeException("Name can not be null");
		}
		this.username = username;
	}
	public String getConnectorStateClass() {
		return this.connectorStateClass;
	}
	public void setConnectorStateClass(String connectorStateClass) {
		this.connectorStateClass = connectorStateClass;
	}
	public String getPassword() {
		return this.password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public URL getURL() {
		return this.URL;
	}
	
	public void setURL(String uRL) {
		try {
			this.URL = new URL(uRL);
		} catch (MalformedURLException e) {
			throw new MetaMatrixRuntimeException("URL Supplied is not valid URL"+ e.getMessage());
		}
	}
	
	public long getSourceConnectionTestInterval() {
		return sourceConnectionTestInterval;
	}
	public void setSourceConnectionTestInterval(Long sourceConnectionTestInterval) {
		this.sourceConnectionTestInterval = sourceConnectionTestInterval.longValue();
	}
	public int getSourceConnectionTimeout() {
		return sourceConnectionTimeout;
	}
	public void setSourceConnectionTimeout(Integer sourceConnectionTimeout) {
		this.sourceConnectionTimeout = sourceConnectionTimeout.intValue();
	}
	public void setModelAuditFields(Boolean modelAuditFields) {
		this.auditModelFields = modelAuditFields.booleanValue();
	}
	public boolean isModelAuditFields() {
		return this.auditModelFields;
	}	
	
	@Override
	public Object createConnectionFactory() throws ResourceException {
		return new BasicConnectionFactory() {
			@Override
			public SalesforceConnectionImpl getConnection() throws ResourceException {
				return new SalesforceConnectionImpl(getUsername(), getPassword(), getURL(), getSourceConnectionTestInterval(), getSourceConnectionTimeout());
			}
		};
	}
}
